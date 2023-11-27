package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.util.BinaryData
import com.azure.storage.queue.QueueAsyncClient
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Dead letter queue async client wrapper. This component expose method to send event to dead letter queue
 * and create, contextually, a new open telemetry span with error context
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
        const val DEAD_LETTER_EVENT_SERVICE_NAME_KEY: String = "deadLetterEvent.serviceName"
        const val DEAD_LETTER_EVENT_SERVICE_NAME_VALUE: String = "pagopa-ecommerce-event-dispatcher-service"
        const val DEAD_LETTER_EVENT_SERVICE_ERROR_CATEGORY_KEY: String = "deadLetterEvent.category"
        const val DEAD_LETTER_EVENT_SERVICE_TRANSACTION_ID_KEY: String = "deadLetterEvent.transactionId"
        const val DEAD_LETTER_EVENT_SERVICE_TRANSACTION_EVENT_CODE_KEY: String = "deadLetterEvent.transactionEventCode"
    }

    /**
     * Send event to dead letter queue creating a new open telemetry span filled with information taken from input error context
     */
    fun sendAndTraceDeadLetterQueueEvent(
        binaryData: BinaryData,
        errorContext: ErrorContext
    ): Mono<Unit> =
        Mono.using(
            { openTelemetryTracer.spanBuilder("Event written into dead letter queue").startSpan() },
            {
                it.setAttribute(DEAD_LETTER_EVENT_SERVICE_NAME_KEY, DEAD_LETTER_EVENT_SERVICE_NAME_VALUE)
                it.setAttribute(DEAD_LETTER_EVENT_SERVICE_ERROR_CATEGORY_KEY, errorContext.errorCategory.toString())
                it.setAttribute(
                    DEAD_LETTER_EVENT_SERVICE_TRANSACTION_ID_KEY,
                    errorContext.transactionId?.toString() ?: "N/A"
                )
                it.setAttribute(
                    DEAD_LETTER_EVENT_SERVICE_TRANSACTION_EVENT_CODE_KEY,
                    errorContext.transactionEventCode?.toString() ?: "N/A"
                )
                deadLetterQueueAsyncClient
                    .sendMessageWithResponse(
                        binaryData,
                        Duration.ZERO,
                        Duration.ofSeconds(deadLetterTTLSeconds.toLong()), // timeToLive
                    )
                    .doOnSuccess { queueResponse -> logger.info("Event: [$binaryData] successfully sent with visibility timeout: [${queueResponse.value.timeNextVisible}] ms to queue: [${deadLetterQueueAsyncClient.queueName}]") }
                    .doOnError { logger.error("Error sending event: [$binaryData] to dead letter queue.") }
                    .then(mono {})
            },
            { it.end() }
        )

    /**
     * Categorization of all errors that can trigger an event to be written to dead letter queue
     */
    enum class ErrorCategory {

        /**
         * Event processing error caused by timeout expiration waiting for send payment result outcome to be received by Nodo
         */
        SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT,

        /**
         * Event processing error caused by no attempt left for processing a retry event
         */
        RETRY_EVENT_NO_ATTEMPT_LEFT,

        /**
         * Event processing error caused by input event processing error
         */
        EVENT_PARSING_ERROR,

        /**
         * Event processing error caused by generic processing error
         */
        PROCESSING_ERROR
    }

    /**
     * This data class contains error context that trigger dead letter event writing
     */
    data class ErrorContext(
        val transactionId: TransactionId?,
        val transactionEventCode: TransactionEventCode?,
        val errorCategory: ErrorCategory

    )
}