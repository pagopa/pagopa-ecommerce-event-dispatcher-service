package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.*
import it.pagopa.ecommerce.commons.domain.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.Transaction
import it.pagopa.ecommerce.commons.domain.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.TransactionUtils
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.util.*

/**
 * Event consumer for expiration events.
 * These events are input in the event queue only when a transaction
 * is stuck in an EXPIRED state **and** needs to be reverted
 */
@Service
class TransactionExpiredEventsConsumer(
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired private val transactionsRefundedEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
) {

    var logger: Logger = LoggerFactory.getLogger(TransactionExpiredEventsConsumer::class.java)

    private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
        val idFromActivatedEvent = data.toObjectAsync(TransactionExpiredEvent::class.java).map { it.transactionId }
        val idFromRefundedEvent = data.toObjectAsync(TransactionRefundRetriedEvent::class.java).map { it.transactionId }

        return Mono.firstWithValue(idFromActivatedEvent, idFromRefundedEvent)
    }

    @ServiceActivator(inputChannel = "transactionexpiredchannel", outputChannel = "nullChannel")
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer
    ): Mono<Void> {
        val checkpoint = checkpointer.success()

        val transactionId = getTransactionIdFromPayload(BinaryData.fromBytes(payload))

        val refundPipeline = transactionId
            .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it) }
            .reduce(EmptyTransaction(), Transaction::applyEvent)
            .cast(BaseTransaction::class.java)
//            .filter { it.status == TransactionStatusDto.EXPIRED } // FIXME reconstruction of EXPIRED transaction
            .doOnNext {
                logger.info("Handling expired transaction with id ${it.transactionId.value}")
            }
            .flatMap {
                try {
                    return@flatMap Mono.just(it as BaseTransactionWithRequestedAuthorization)
                } catch (e: ClassCastException) {
                    return@flatMap Mono.empty()
                }
            }
            .switchIfEmpty {
                return@switchIfEmpty transactionId.doOnNext {
                    logger.info("Transaction $it was not previously authorized. No refund needed")
                }.flatMap { Mono.empty() }
            }
            .flatMap { transaction ->
                val authorizationRequestId =
                    transaction.transactionAuthorizationRequestData.authorizationRequestId

                paymentGatewayClient.requestRefund(
                    UUID.fromString(authorizationRequestId)
                ).map { refundResponse -> Pair(refundResponse, transaction) }
            }
            .flatMap {
                val (refundResponse, transaction) = it
                logger.info(
                    "Transaction requestRefund for transaction ${transaction.transactionId} with outcome ${refundResponse.refundOutcome}"
                )
                when (refundResponse.refundOutcome) {
                    "OK" -> updateTransactionToRefunded(transaction)
                    else ->
                        Mono.error(RuntimeException("Refund error for transaction ${transaction.transactionId} with outcome  ${refundResponse.refundOutcome}"))
                }
            }
            .onErrorMap {
                logger.error(
                    "Transaction requestRefund error for transaction ${transactionId.block()} : ${it.message}"
                )
                // TODO retry
                it
            }

        return checkpoint.then(refundPipeline).then()
    }

    private fun updateTransactionToRefunded(transaction: BaseTransaction): Mono<BaseTransaction> {
        return transactionsRefundedEventStoreRepository.save(
            TransactionRefundedEvent(
                transaction.transactionId.value.toString(),
                TransactionRefundedData(transaction.status)
            )
        ).then(
            transactionsViewRepository.save(
                Transaction(
                    transaction.transactionId.value.toString(),
                    paymentNoticeDocuments(transaction.paymentNotices),
                    TransactionUtils.getTransactionFee(transaction).orElse(null),
                    transaction.email.value,
                    TransactionStatusDto.EXPIRED,
                    transaction.clientId,
                    transaction.creationDate.toString()
                )
            )
        ).doOnSuccess {
            logger.info(
                "Transaction refunded for transaction ${transaction.transactionId.value}"
            )
        }.thenReturn(transaction)
    }

    private fun paymentNoticeDocuments(paymentNotices: List<it.pagopa.ecommerce.commons.domain.PaymentNotice>): List<PaymentNotice> {
        return paymentNotices.map { notice ->
            PaymentNotice(
                notice.paymentToken.value,
                notice.rptId.value,
                notice.transactionDescription.value,
                notice.transactionAmount.value,
                notice.paymentContextCode.value
            )
        }
    }
}
