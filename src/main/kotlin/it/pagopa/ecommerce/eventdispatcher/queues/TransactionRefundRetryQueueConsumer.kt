package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.*

/**
 * Event consumer for events related to refund retry. This consumer's responsibilities are to handle
 * refund process retry for a given transaction
 */
@Service
class TransactionRefundRetryQueueConsumer(
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired
    private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val refundRetryService: RefundRetryService,
    @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
    @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
    private val deadLetterTTLSeconds: Int,
    @Autowired private val tracingUtils: TracingUtils
) {

    var logger: Logger = LoggerFactory.getLogger(TransactionRefundRetryQueueConsumer::class.java)

    private fun parseEvent(
        data: BinaryData
    ): Mono<Pair<TransactionRefundRetriedEvent, TracingInfo?>> {
        val refundRetriedEvent =
            data
                .toObjectAsync(object : TypeReference<QueueEvent<TransactionRefundRetriedEvent>>() {})
                .map { it.event to it.tracingInfo }

        val untracedRefundRetriedEvent =
            data.toObjectAsync(object : TypeReference<TransactionRefundRetriedEvent>() {}).map {
                it to null
            }

        return Mono.firstWithValue(refundRetriedEvent, untracedRefundRetriedEvent)
    }

    @ServiceActivator(inputChannel = "transactionrefundretrychannel", outputChannel = "nullChannel")
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
    ): Mono<Void> {
        val binaryData = BinaryData.fromBytes(payload)
        val event = parseEvent(binaryData)
        val baseTransaction =
            event
                .flatMapMany {
                    transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
                        it.first.transactionId
                    )
                }
                .reduce(EmptyTransaction(), Transaction::applyEvent)
                .cast(BaseTransaction::class.java)
        val refundPipeline =
            baseTransaction
                .flatMap {
                    logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

                    if (it.status != TransactionStatusDto.REFUND_ERROR) {
                        Mono.error(
                            BadTransactionStatusException(
                                transactionId = it.transactionId,
                                expected = listOf(TransactionStatusDto.REFUND_ERROR),
                                actual = it.status
                            )
                        )
                    } else {
                        Mono.just(it)
                    }
                }
                .flatMap { tx ->
                    event.flatMap { (e, tracingInfo) ->
                        refundTransaction(
                            tx,
                            transactionsRefundedEventStoreRepository,
                            transactionsViewRepository,
                            paymentGatewayClient,
                            refundRetryService,
                            tracingInfo,
                            e.data.retryCount
                        )
                    }
                }
        return event
            .onErrorMap { InvalidEventException(payload) }
            .flatMap { (e, tracingInfo) ->
                if (tracingInfo != null) {
                    runTracedPipelineWithDeadLetterQueue(
                        checkPointer,
                        refundPipeline,
                        QueueEvent(e, tracingInfo),
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds,
                        tracingUtils,
                        this::class.simpleName!!
                    )
                } else {
                    runPipelineWithDeadLetterQueue(
                        checkPointer,
                        refundPipeline,
                        BinaryData.fromObject(e).toBytes(),
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds,
                    )
                }
            }.onErrorResume(InvalidEventException::class.java) {
                logger.error("Invalid input event", it)
                runPipelineWithDeadLetterQueue(
                    checkPointer,
                    refundPipeline,
                    payload,
                    deadLetterQueueAsyncClient,
                    deadLetterTTLSeconds
                )
            }
    }
}
