package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithUserReceiptError
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.UserReceiptMailBuilder
import it.pagopa.generated.notifications.templates.success.*
import kotlinx.coroutines.reactor.mono
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

@Service
class TransactionNotificationsRetryQueueConsumer(
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired
    private val transactionUserReceiptRepository:
    TransactionsEventStoreRepository<TransactionUserReceiptData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val notificationRetryService: NotificationRetryService,
    @Autowired
    private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val refundRetryService: RefundRetryService,
    @Autowired private val userReceiptMailBuilder: UserReceiptMailBuilder,
    @Autowired private val notificationsServiceClient: NotificationsServiceClient,
    @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
    @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
    private val deadLetterTTLSeconds: Int,
    @Autowired private val tracingUtils: TracingUtils
) {
    var logger: Logger =
        LoggerFactory.getLogger(TransactionNotificationsRetryQueueConsumer::class.java)

    private fun getTransactionIdFromPayload(
        event: Either<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>
    ): String {
        return event.fold({ it.transactionId }, { it.transactionId })
    }

    private fun getRetryCountFromPayload(
        event: Either<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>
    ): Int {
        return event.fold({ 0 }, { Optional.ofNullable(it.data.retryCount).orElse(0) })
    }

    private fun parseEvent(
        data: BinaryData
    ): Mono<
            Pair<
                    Either<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>,
                    TracingInfo?>> {
        val notificationErrorEvent =
            data
                .toObjectAsync(object : TypeReference<QueueEvent<TransactionUserReceiptAddErrorEvent>>() {})
                .map {
                    Either.left<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>(
                        it.event
                    ) to it.tracingInfo
                }

        val notificationRetryEvent =
            data
                .toObjectAsync(
                    object : TypeReference<QueueEvent<TransactionUserReceiptAddRetriedEvent>>() {})
                .map {
                    Either.right<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>(
                        it.event
                    ) to it.tracingInfo
                }

        val untracedNotificationErrorEvent =
            data.toObjectAsync(object : TypeReference<TransactionUserReceiptAddErrorEvent>() {}).map {
                Either.left<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>(
                    it
                ) to null
            }

        val untracedNotificationRetryEvent =
            data.toObjectAsync(object : TypeReference<TransactionUserReceiptAddRetriedEvent>() {}).map {
                Either.right<TransactionUserReceiptAddErrorEvent, TransactionUserReceiptAddRetriedEvent>(
                    it
                ) to null
            }

        return Mono.firstWithValue(
            notificationErrorEvent,
            notificationRetryEvent,
            untracedNotificationErrorEvent,
            untracedNotificationRetryEvent
        )
    }

    @ServiceActivator(
        inputChannel = "transactionretrynotificationschannel", outputChannel = "nullChannel"
    )
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
    ): Mono<Void> {
        val binaryData = BinaryData.fromBytes(payload)
        val eventData = parseEvent(binaryData)
        val transactionId = eventData.map { getTransactionIdFromPayload(it.first) }
        val retryCount = eventData.map { getRetryCountFromPayload(it.first) }
        val baseTransaction =
            reduceEvents(transactionId, transactionsEventStoreRepository, EmptyTransaction())
        val notificationResendPipeline =
            baseTransaction
                .flatMap {
                    logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

                    if (it.status != TransactionStatusDto.NOTIFICATION_ERROR) {
                        Mono.error(
                            BadTransactionStatusException(
                                transactionId = it.transactionId,
                                expected = listOf(TransactionStatusDto.NOTIFICATION_ERROR),
                                actual = it.status
                            )
                        )
                    } else {
                        Mono.just(it)
                    }
                }
                .cast(TransactionWithUserReceiptError::class.java)
                .flatMap { tx ->
                    mono { userReceiptMailBuilder.buildNotificationEmailRequestDto(tx) }
                        .flatMap { notificationsServiceClient.sendNotificationEmail(it) }
                        .zipWith(eventData, ::Pair)
                        .flatMap { (_, eventData) ->
                            updateNotifiedTransactionStatus(
                                tx, transactionsViewRepository, transactionUserReceiptRepository
                            )
                                .flatMap {
                                    val tracingInfo = eventData.second

                                    notificationRefundTransactionPipeline(
                                        tx,
                                        transactionsRefundedEventStoreRepository,
                                        transactionsViewRepository,
                                        paymentGatewayClient,
                                        refundRetryService,
                                        tracingInfo
                                    )
                                }
                        }
                        .then()
                        .onErrorResume { exception ->
                            logger.error(
                                "Got exception while retrying user receipt mail sending for transaction with id ${tx.transactionId}!",
                                exception
                            )
                            retryCount
                                .zipWith(eventData, ::Pair)
                                .flatMap { (retryCount, eventData) ->
                                    val tracingInfo = eventData.second
                                    val v = notificationRetryService.enqueueRetryEvent(tx, retryCount, tracingInfo)
                                    logger.info("enq response: {}", v)
                                    v
                                }
                                .onErrorResume(NoRetryAttemptsLeftException::class.java) { enqueueException ->
                                    logger.error(
                                        "No more attempts left for user receipt send retry", enqueueException
                                    )

                                    eventData.flatMap {
                                        notificationRefundTransactionPipeline(
                                            tx,
                                            transactionsRefundedEventStoreRepository,
                                            transactionsViewRepository,
                                            paymentGatewayClient,
                                            refundRetryService,
                                            it.second
                                        )
                                            .then()
                                    }
                                }
                                .then()
                        }
                }

        return eventData
            .onErrorMap { InvalidEventException(payload) }
            .flatMap { (e, tracingInfo) ->
                if (tracingInfo != null) {
                    val event = e.fold({ QueueEvent(it, tracingInfo) }, { QueueEvent(it, tracingInfo) })

                    runTracedPipelineWithDeadLetterQueue(
                        checkPointer,
                        notificationResendPipeline,
                        event,
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds,
                        tracingUtils,
                        this::class.simpleName!!
                    )
                } else {
                    val eventBytes =
                        e.fold({ BinaryData.fromObject(it).toBytes() }, { BinaryData.fromObject(it).toBytes() })

                    runPipelineWithDeadLetterQueue(
                        checkPointer,
                        notificationResendPipeline,
                        eventBytes,
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds
                    )
                }
            }.onErrorResume(InvalidEventException::class.java) {
                logger.error("Invalid input event", it)
                runPipelineWithDeadLetterQueue(
                    checkPointer,
                    notificationResendPipeline,
                    payload,
                    deadLetterQueueAsyncClient,
                    deadLetterTTLSeconds
                )
            }

    }
}
