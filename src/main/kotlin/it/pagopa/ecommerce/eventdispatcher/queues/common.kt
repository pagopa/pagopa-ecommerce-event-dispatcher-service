package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets

object QueueCommonsLogger {
    val logger: Logger = LoggerFactory.getLogger(QueueCommonsLogger::class.java)
}

const val ERROR_PARSING_EVENT_ERROR = "Cannot parse event"

fun writeEventToDeadLetterQueue(
    checkPointer: Checkpointer,
    eventPayload: ByteArray,
    exception: Throwable,
    deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
    errorContext: DeadLetterTracedQueueAsyncClient.ErrorContext
): Mono<Unit> {
    val binaryData = BinaryData.fromBytes(eventPayload)
    val eventLogString = "event payload: ${eventPayload.toString(StandardCharsets.UTF_8)}"

    QueueCommonsLogger.logger.error("Exception processing event $eventLogString", exception)

    return checkPointer
        .success()
        .doOnSuccess {
            QueueCommonsLogger.logger.info("Checkpoint performed successfully for event $eventLogString")
        }
        .doOnError {
            QueueCommonsLogger.logger.error("Error performing checkpoint for event $eventLogString", it)
        }
        .then(
            deadLetterTracedQueueAsyncClient
                .sendAndTraceDeadLetterQueueEvent(
                    binaryData = binaryData,
                    errorContext = errorContext,
                )
        )
}
