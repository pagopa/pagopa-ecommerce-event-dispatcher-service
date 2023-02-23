package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.*
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
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
import java.util.*

/**
 * Event consumer for events related to transaction activation.
 * This consumer's responsibilities are to handle expiration of transactions and subsequent refund
 * for transaction stuck in a pending/transient state.
 */
@Service
class TransactionActivatedEventsConsumer(
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired private val transactionsExpiredEventStoreRepository: TransactionsEventStoreRepository<TransactionExpiredData>,
    @Autowired private val transactionsRefundedEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val transactionUtils: TransactionUtils
) {

    var logger: Logger = LoggerFactory.getLogger(TransactionActivatedEventsConsumer::class.java)

    private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
        val idFromActivatedEvent = data.toObjectAsync(TransactionActivatedEvent::class.java).map { it.transactionId }
        val idFromRefundedEvent = data.toObjectAsync(TransactionRefundRetriedEvent::class.java).map { it.transactionId }

        return Mono.firstWithValue(idFromActivatedEvent, idFromRefundedEvent)
    }

    @ServiceActivator(inputChannel = "transactionactivatedchannel", outputChannel = "nullChannel")
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer
    ): Mono<Void> {
        val checkpoint = checkpointer.success()

        val transactionId = getTransactionIdFromPayload(BinaryData.fromBytes(payload))

        val refundPipeline = transactionId
            .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it) }
            .reduce(EmptyTransaction(), Transaction::applyEvent).cast(BaseTransaction::class.java)
            .filter {
                transactionUtils.isTransientStatus(it.status)
            }
            .flatMap(::updateTransactionToExpired)
            .filter {
                val refundable = transactionUtils.isRefundableTransaction(it.status)
                logger.info("Transaction ${it.transactionId.value} in status ${it.status}, refundable: $refundable")
                refundable
            }
            .cast(BaseTransactionWithRequestedAuthorization::class.java)
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
            .onErrorMap { exception ->
                transactionId.map { id ->
                    logger.error(
                        "Transaction requestRefund error for transaction $id : ${exception.message}"
                    )
                }

                // TODO retry
                exception
            }

        return checkpoint.then(refundPipeline).then()
    }

    private fun updateTransactionToExpired(transaction: BaseTransaction): Mono<BaseTransaction> {

        return transactionsExpiredEventStoreRepository.save(
            TransactionExpiredEvent(
                transaction.transactionId.value.toString(),
                TransactionExpiredData(transaction.status)
            )
        ).then(
            transactionsViewRepository.save(
                Transaction(
                    transaction.transactionId.value.toString(),
                    paymentNoticeDocuments(transaction.paymentNotices),
                    TransactionUtils.getTransactionFee(transaction).orElse(null),
                    transaction.email,
                    TransactionStatusDto.EXPIRED,
                    transaction.clientId,
                    transaction.creationDate.toString()
                )
            )
        ).doOnSuccess {
            logger.info(
                "Transaction expired for transaction ${transaction.transactionId.value}"
            )
        }.doOnError {
            logger.error(
                "Transaction expired error for transaction ${transaction.transactionId.value} : ${it.message}"
            )
        }.thenReturn(transaction)
    }

    private fun updateTransactionToRefunded(transaction: BaseTransaction): Mono<BaseTransaction> {

        return transactionsRefundedEventStoreRepository.save(
            TransactionRefundedEvent(
                transaction.transactionId.value.toString(),
                TransactionRefundedData(TransactionStatusDto.EXPIRED)
            )
        ).then(
            transactionsViewRepository.save(
                Transaction(
                    transaction.transactionId.value.toString(),
                    paymentNoticeDocuments(transaction.paymentNotices),
                    TransactionUtils.getTransactionFee(transaction).orElse(null),
                    transaction.email,
                    TransactionStatusDto.REFUNDED,
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

    private fun paymentNoticeDocuments(paymentNotices: List<it.pagopa.ecommerce.commons.domain.v1.PaymentNotice>): List<PaymentNotice> {
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
