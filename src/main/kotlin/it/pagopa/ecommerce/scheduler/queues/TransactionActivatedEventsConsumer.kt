package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
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
        val idFromClosedEvent = data.toObjectAsync(TransactionClosedEvent::class.java).map { it.transactionId }

        return Mono.firstWithValue(idFromActivatedEvent, idFromClosedEvent)
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
            .reduce(EmptyTransaction(), Transaction::applyEvent)
            .cast(BaseTransaction::class.java)
            .filter {
                transactionUtils.isTransientStatus(it.status)
            }
            .flatMap { tx -> updateTransactionToExpired(tx, transactionsExpiredEventStoreRepository, transactionsViewRepository) }
            .filter {
                val refundable = isTransactionRefundable(it)
                logger.info("Transaction ${it.transactionId.value} in status ${it.status}, refundable: $refundable")
                refundable
            }
            .flatMap { tx -> refundTransaction(tx, transactionsRefundedEventStoreRepository, transactionsViewRepository, paymentGatewayClient) }
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
}
