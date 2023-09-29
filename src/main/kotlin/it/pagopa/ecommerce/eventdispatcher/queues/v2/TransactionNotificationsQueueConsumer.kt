package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.v2.UserReceiptMailBuilder
import it.pagopa.generated.notifications.templates.success.*
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionNotificationsQueueConsumerv2")
class TransactionNotificationsQueueConsumer(
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionUserReceiptRepository:
    TransactionsEventStoreRepository<TransactionUserReceiptData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val notificationRetryService: NotificationRetryService,
  @Autowired private val userReceiptMailBuilder: UserReceiptMailBuilder,
  @Autowired private val notificationsServiceClient: NotificationsServiceClient,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionNotificationsQueueConsumer::class.java)

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionUserReceiptRequestedEvent>,
    checkPointer: Checkpointer
  ) = messageReceiver(parsedEvent, checkPointer, EmptyTransaction())

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionUserReceiptRequestedEvent>,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val event = parsedEvent.event
    val tracingInfo = parsedEvent.tracingInfo
    val transactionId = event.transactionId
    val baseTransaction =
      reduceEvents(mono { transactionId }, transactionsEventStoreRepository, emptyTransaction)
    val notificationResendPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.NOTIFICATION_REQUESTED) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = listOf(TransactionStatusDto.NOTIFICATION_REQUESTED),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .cast(BaseTransactionWithRequestedUserReceipt::class.java)
        .flatMap { tx ->
          mono { userReceiptMailBuilder.buildNotificationEmailRequestDto(tx) }
            .flatMap { notificationsServiceClient.sendNotificationEmail(it) }
            .flatMap {
              updateNotifiedTransactionStatus(
                tx, transactionsViewRepository, transactionUserReceiptRepository)
            }
            .flatMap {
              notificationRefundTransactionPipeline(
                tx,
                transactionsRefundedEventStoreRepository,
                transactionsViewRepository,
                paymentGatewayClient,
                refundRetryService,
                tracingInfo)
            }
            .then()
            .onErrorResume { exception ->
              logger.error(
                "Got exception while retrying user receipt mail sending for transaction with id ${tx.transactionId}!",
                exception)
              updateNotificationErrorTransactionStatus(
                  tx, transactionsViewRepository, transactionUserReceiptRepository)
                .flatMap {
                  notificationRetryService.enqueueRetryEvent(tx, 0, tracingInfo).doOnError {
                    retryException ->
                    logger.error("Exception enqueueing notification retry event", retryException)
                  }
                }
                .then()
            }
        }

    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      notificationResendPipeline,
      QueueEvent(event, tracingInfo),
      deadLetterQueueAsyncClient,
      deadLetterTTLSeconds,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }
}
