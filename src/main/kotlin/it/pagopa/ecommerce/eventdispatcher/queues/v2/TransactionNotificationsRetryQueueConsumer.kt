package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithUserReceiptError
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.v2.UserReceiptMailBuilder
import it.pagopa.generated.notifications.templates.success.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.*

@Service("TransactionNotificationsRetryQueueConsumerV2")
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
    @Autowired private val refundService: RefundService,
    @Autowired private val refundRetryService: RefundRetryService,
    @Autowired private val userReceiptMailBuilder: UserReceiptMailBuilder,
    @Autowired private val notificationsServiceClient: NotificationsServiceClient,
    @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
    @Autowired private val tracingUtils: TracingUtils,
    @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
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

    fun messageReceiver(
        parsedEvent:
        Either<
                QueueEvent<TransactionUserReceiptAddErrorEvent>,
                QueueEvent<TransactionUserReceiptAddRetriedEvent>>,
        checkPointer: Checkpointer
    ): Mono<Unit> {
        val event = parsedEvent.bimap({ it.event }, { it.event })
        val tracingInfo = parsedEvent.fold({ it.tracingInfo }, { it.tracingInfo })
        val queueEvent = parsedEvent.fold({ it }, { it })
        val transactionId = getTransactionIdFromPayload(event)
        val retryCount = getRetryCountFromPayload(event)
        val baseTransaction =
            reduceEvents(mono { transactionId }, transactionsEventStoreRepository, EmptyTransaction())
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
                        .flatMap {
                            updateNotifiedTransactionStatus(
                                tx, transactionsViewRepository, transactionUserReceiptRepository
                            )
                                .flatMap {
                                    notificationRefundTransactionPipeline(
                                        tx,
                                        transactionsRefundedEventStoreRepository,
                                        transactionsViewRepository,
                                        paymentGatewayClient,
                                        refundService,
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
                            val v = notificationRetryService.enqueueRetryEvent(tx, retryCount, tracingInfo)
                            v.onErrorResume(NoRetryAttemptsLeftException::class.java) { enqueueException ->
                                logger.error(
                                    "No more attempts left for user receipt send retry", enqueueException
                                )
                                BinaryData.fromObjectAsync(
                                    queueEvent, strictSerializerProviderV2.createInstance()
                                )
                                    .flatMap {
                                        deadLetterTracedQueueAsyncClient
                                            .sendAndTraceDeadLetterQueueEvent(
                                                it,
                                                DeadLetterTracedQueueAsyncClient.ErrorContext(
                                                    transactionId = TransactionId(queueEvent.event.transactionId),
                                                    transactionEventCode = TransactionEventCode.valueOf(queueEvent.event.eventCode),
                                                    DeadLetterTracedQueueAsyncClient.ErrorCategory.RETRY_EVENT_NO_ATTEMPT_LEFT
                                                )
                                            )
                                    }
                                    .onErrorResume {
                                        logger.error("Error writing event to dead letter queue", it)
                                        Mono.empty()
                                    }
                                    .then(
                                        notificationRefundTransactionPipeline(
                                            tx,
                                            transactionsRefundedEventStoreRepository,
                                            transactionsViewRepository,
                                            paymentGatewayClient,
                                            refundService,
                                            refundRetryService,
                                            tracingInfo
                                        )
                                    )
                                    .then()
                            }
                                .then()
                        }
                }

        return runTracedPipelineWithDeadLetterQueue(
            checkPointer,
            notificationResendPipeline,
            queueEvent,
            deadLetterTracedQueueAsyncClient,
            tracingUtils,
            this::class.simpleName!!,
            strictSerializerProviderV2
        )
    }
}
