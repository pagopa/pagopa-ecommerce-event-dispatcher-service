package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.generated.transactions.server.model.TransactionStatusDto
import it.pagopa.transactions.documents.*
import it.pagopa.transactions.domain.EmptyTransaction
import it.pagopa.transactions.domain.Transaction
import it.pagopa.transactions.domain.pojos.BaseTransaction
import it.pagopa.transactions.domain.pojos.BaseTransactionWithRequestedAuthorization
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import java.util.Objects
import java.util.UUID

@Service
class TransactionActivatedEventsConsumer(
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Objects>,
    @Autowired private val transactionsExpiredEventStoreRepository: TransactionsEventStoreRepository<TransactionExpiredData>,
    @Autowired private val transactionsRefundedEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository
) {

    var logger: Logger = LoggerFactory.getLogger(TransactionActivatedEventsConsumer::class.java)

    @ServiceActivator(inputChannel = "transactionactivatedchannel")
    fun messageReceiver(@Payload payload: ByteArray, @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer) {
        checkpointer.success().block()

        val activatedEvent =
            BinaryData.fromBytes(payload).toObject(TransactionActivatedEvent::class.java)
        val transactionId = activatedEvent.transactionId
        val paymentToken = activatedEvent.paymentToken

        transactionsEventStoreRepository.findByTransactionId(transactionId.toString())
            .reduce(EmptyTransaction(), Transaction::applyEvent).cast(BaseTransaction::class.java)
            .filter {
                isTransientStatus(it.status)
            }
            .flatMap { transaction ->
                updateTransactionToExpired(transaction, paymentToken)
                    .doOnSuccess {
                        logger.info(
                            "Transaction expired for transaction ${transaction.transactionId.value}"
                        )
                    }.doOnError {
                        logger.error(
                            "Transaction expired error for transaction ${transaction.transactionId.value} : ${it.message}"
                        )
                    }.thenReturn(transaction)
            }
            .filter {
                isRefundableTransaction(it.status)
            }
            .flatMap { transaction ->
                mono { transaction }.cast(BaseTransactionWithRequestedAuthorization::class.java)
                    .flatMap {
                        val authorizationRequestId =
                            it.transactionAuthorizationRequestData.authorizationRequestId
                        paymentGatewayClient.requestRefund(
                            UUID.fromString(authorizationRequestId)
                        )
                    }.flatMap {
                        logger.info(
                            "Transaction requestRefund for transaction $transactionId with outcome ${it.refundOutcome}"
                        )
                        when (it.refundOutcome) {
                            "OK" -> mono { updateTransactionToRefunded(transaction, paymentToken) }
                                .doOnSuccess {
                                    logger.info(
                                        "Transaction refunded for transaction ${transaction.transactionId.value}"
                                    )
                                }.block()
                            else ->
                                error("Refund error for transaction $transactionId with outcome  ${it.refundOutcome}")
                        }
                    }.onErrorMap {
                        logger.error(
                            "Transaction requestRefund error for transaction $transactionId : ${it.message}"
                        )
                        // TODO retry
                        it
                    }
            }.subscribe()
    }

    private fun updateTransactionToExpired(transaction: BaseTransaction, paymentToken: String): Mono<Void> {

        return transactionsExpiredEventStoreRepository.save(
            TransactionExpiredEvent(
                transaction.transactionId.value.toString(),
                transaction.rptId.value,
                paymentToken,
                TransactionExpiredData(transaction.status)
            )
        ).then(
            transactionsViewRepository.save(
                Transaction(
                    transaction.transactionId.value.toString(),
                    paymentToken,
                    transaction.rptId.value,
                    transaction.description.value,
                    transaction.amount.value,
                    transaction.email.value,
                    TransactionStatusDto.EXPIRED
                )
            ).then()
        )
    }

    private fun updateTransactionToRefunded(transaction: BaseTransaction, paymentToken: String): Mono<Void> {

        return transactionsRefundedEventStoreRepository.save(
            TransactionRefundedEvent(
                transaction.transactionId.value.toString(),
                transaction.rptId.value,
                paymentToken,
                TransactionRefundedData(transaction.status)
            )
        ).then(
            transactionsViewRepository.save(
                it.pagopa.transactions.documents.Transaction(
                    transaction.transactionId.value.toString(),
                    paymentToken,
                    transaction.rptId.value,
                    transaction.description.value,
                    transaction.amount.value,
                    transaction.email.value,
                    TransactionStatusDto.REFUNDED
                )
            ).then()
        )
    }

    private fun isTransientStatus(status: TransactionStatusDto): Boolean {
        return TransactionStatusDto.ACTIVATED == status
                || TransactionStatusDto.AUTHORIZED == status
                || TransactionStatusDto.AUTHORIZATION_REQUESTED == status
                || TransactionStatusDto.AUTHORIZATION_FAILED == status
                || TransactionStatusDto.CLOSURE_FAILED == status
                || TransactionStatusDto.CLOSED == status
    }

    private fun isRefundableTransaction(status: TransactionStatusDto): Boolean {
        return TransactionStatusDto.AUTHORIZED == status
                || TransactionStatusDto.AUTHORIZATION_REQUESTED == status
                || TransactionStatusDto.AUTHORIZATION_FAILED == status
                || TransactionStatusDto.CLOSURE_FAILED == status
    }


}
