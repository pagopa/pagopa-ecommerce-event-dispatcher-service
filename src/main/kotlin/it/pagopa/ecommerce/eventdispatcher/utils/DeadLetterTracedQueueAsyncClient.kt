package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.util.BinaryData
import com.azure.storage.queue.QueueAsyncClient
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.domain.TransactionId
import java.time.Duration
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * Dead letter queue async client wrapper. This component exposes a method to send an event to a dead letter
 * queue and contextually creates a new OpenTelemetry span with an error context
 */
@Component
class DeadLetterTracedQueueAsyncClient(
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val openTelemetryTracer: Tracer
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  companion object {
    private val DEAD_LETTER_EVENT_SERVICE_NAME_KEY: AttributeKey<String> =
      AttributeKey.stringKey("deadLetterEvent.serviceName")
    private const val DEAD_LETTER_EVENT_SERVICE_NAME_VALUE: String =
      "pagopa-ecommerce-event-dispatcher-service"
    private val DEAD_LETTER_EVENT_SERVICE_ERROR_CATEGORY_KEY: AttributeKey<String> =
      AttributeKey.stringKey("deadLetterEvent.category")
    private val DEAD_LETTER_EVENT_SERVICE_TRANSACTION_ID_KEY: AttributeKey<String> =
      AttributeKey.stringKey("deadLetterEvent.transactionId")
    private val DEAD_LETTER_EVENT_SERVICE_TRANSACTION_EVENT_CODE_KEY: AttributeKey<String> =
      AttributeKey.stringKey("deadLetterEvent.transactionEventCode")
  }

  /**
   * Send event to dead letter queue creating a new open telemetry span filled with information
   * taken from input error context
   */
  fun sendAndTraceDeadLetterQueueEvent(
    binaryData: BinaryData,
    errorContext: ErrorContext
  ): Mono<Unit> =
    Mono.using(
      { openTelemetryTracer.spanBuilder("Event written into dead letter queue").startSpan() },
      {
        it.setAttribute(DEAD_LETTER_EVENT_SERVICE_NAME_KEY, DEAD_LETTER_EVENT_SERVICE_NAME_VALUE)
        it.setAttribute(
          DEAD_LETTER_EVENT_SERVICE_ERROR_CATEGORY_KEY, errorContext.errorCategory.toString())
        it.setAttribute(
          DEAD_LETTER_EVENT_SERVICE_TRANSACTION_ID_KEY,
          errorContext.transactionId?.value() ?: "N/A")
        it.setAttribute(
          DEAD_LETTER_EVENT_SERVICE_TRANSACTION_EVENT_CODE_KEY,
          errorContext.transactionEventCode ?: "N/A")
        deadLetterQueueAsyncClient
          .sendMessageWithResponse(
            binaryData,
            Duration.ZERO,
            Duration.ofSeconds(deadLetterTTLSeconds.toLong()), // timeToLive
          )
          .doOnSuccess { queueResponse ->
            logger.info(
              "Event: [$binaryData] successfully sent with visibility timeout: [${queueResponse.value.timeNextVisible}] ms to queue: [${deadLetterQueueAsyncClient.queueName}]")
          }
          .doOnError { exception ->
            logger.error("Error sending event: [$binaryData] to dead letter queue.", exception)
          }
          .then(mono {})
      },
      { it.end() })

  /** Categorization of all errors that can trigger an event to be written to dead letter queue */
  enum class ErrorCategory {

    /**
     * Event processing error caused by timeout expiration waiting for send payment result outcome
     * to be received by Nodo
     */
    SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT,

    /** Event processing error caused by no attempts left for processing a retry event */
    RETRY_EVENT_NO_ATTEMPTS_LEFT,

    /** Event processing error caused by input event processing error */
    EVENT_PARSING_ERROR,

    /** Event processing error caused by generic processing error */
    PROCESSING_ERROR
  }

  /** This data class contains the context of the error that triggered the writing of a dead letter event */
  data class ErrorContext(
    val transactionId: TransactionId?,
    val transactionEventCode: String?,
    val errorCategory: ErrorCategory
  )
}
