package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.client.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRefundedData
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptRequestedEvent
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionTracing
import it.pagopa.ecommerce.eventdispatcher.utils.v2.UserReceiptMailBuilder
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
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
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  @Autowired private val refundRequestedAsyncClient: QueueAsyncClient,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val npgService: NpgService,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueueTTLSeconds: Int,
  @Autowired private val openTelemetryUtils: OpenTelemetryUtils
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

    val events =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
        .map { it as TransactionEvent<Any> }

    val baseTransaction = reduceEvents(events, EmptyTransaction())

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
                .doOnSuccess { addSpanAttributesFromTransaction(it, events, openTelemetryUtils) }
            }
            .flatMap {
              notificationRefundTransactionPipeline(
                it,
                transactionsRefundedEventStoreRepository,
                transactionsViewRepository,
                npgService,
                tracingInfo,
                refundRequestedAsyncClient,
                Duration.ofSeconds(transientQueueTTLSeconds.toLong()))
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
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  private fun extractSpanAttributesFromTransaction(tx: BaseTransactionWithUserReceipt): Attributes {
    // Calculate the duration in seconds between creation and final status
    val transactionFinalStatusDate =
      ZonedDateTime.parse(
        tx.transactionUserReceiptAddedEvent.creationDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    val totalDurationInSeconds =
      ChronoUnit.SECONDS.between(tx.creationDate, transactionFinalStatusDate)

    return Attributes.builder()
      .put(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID), tx.transactionId.toString())
      .put(
        AttributeKey.stringKey(TransactionTracing.TRANSACTIONEVENT),
        tx.transactionUserReceiptAddedEvent.eventCode)
      .put(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS), tx.status.value)
      .put(AttributeKey.stringKey(TransactionTracing.CLIENTID), tx.clientId.toString())
      .put(
        AttributeKey.stringKey(TransactionTracing.PSPID),
        tx.transactionAuthorizationRequestData.pspId)
      .put(
        AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD),
        tx.transactionAuthorizationRequestData.paymentMethodName)
      .put(
        AttributeKey.stringKey(TransactionTracing.TRANSACTIONTOTALTIME),
        totalDurationInSeconds.toString())
      .build()
  }

  private fun addSpanAttributesFromTransaction(
    tx: BaseTransactionWithUserReceipt,
    events: Flux<TransactionEvent<Any>>,
    openTelemetryUtils: OpenTelemetryUtils
  ) {
    events
      .collectMap({ event -> event.eventCode }, { event -> event.creationDate })
      .map { eventDateMap ->
        // Calculate durations in milliseconds
        val authorizationDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_AUTHORIZATION_COMPLETED_EVENT.toString()])

        val closePaymentToAddUserReceiptRequestedDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString()])

        val totalDuration =
          calculateDurationMs(
            eventDateMap[TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString()],
            eventDateMap[TransactionEventCode.TRANSACTION_USER_RECEIPT_ADDED_EVENT.toString()])

        Attributes.builder()
          .put(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID), tx.transactionId.value())
          .put(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS), tx.status.value)
          .put(AttributeKey.stringKey(TransactionTracing.CLIENTID), tx.clientId.toString())
          .put(
            AttributeKey.stringKey(TransactionTracing.PSPID),
            tx.transactionAuthorizationRequestData.pspId)
          .put(
            AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD),
            tx.transactionAuthorizationRequestData.paymentMethodName)
          .put(AttributeKey.longKey(TransactionTracing.TRANSACTIONTOTALTIME), totalDuration)
          .put(
            AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME),
            authorizationDuration)
          .put(
            AttributeKey.longKey(TransactionTracing.TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME),
            closePaymentToAddUserReceiptRequestedDuration)
          .build()
      }
      .doOnSuccess { attributes ->
        openTelemetryUtils.addSpanWithAttributes(TransactionTracing::class.simpleName, attributes)
      }
      .doOnError { error ->
        logger.warn("Failed to extract span attributes: ${error.message}", error)
      }
      .subscribe(
        {}, { error -> logger.warn("Unhandled error in span attributes extraction", error) })
  }

  /**
   * Calculates the duration in milliseconds between two date strings.
   *
   * @param startDateString The start date as a string in ISO-8601 format, can be null
   * @param endDateString The end date as a string in ISO-8601 format, can be null
   * @return Duration in milliseconds, or 0 if either date is invalid or null
   */
  private fun calculateDurationMs(startDateString: String?, endDateString: String?): Long {
    val startDate = parseDate(startDateString)
    val endDate = parseDate(endDateString)

    if (startDate == null || endDate == null) return 0
    return Duration.between(startDate, endDate).toMillis()
  }

  /**
   * Helper method to parse a date string to ZonedDateTime.
   *
   * @param dateString The date string in ISO-8601 format
   * @return Parsed ZonedDateTime or null if the string is null, empty, or invalid
   */
  private fun parseDate(dateString: String?): ZonedDateTime? {
    if (dateString.isNullOrEmpty()) return null
    return try {
      ZonedDateTime.parse(dateString)
    } catch (e: Exception) {
      null
    }
  }
}
