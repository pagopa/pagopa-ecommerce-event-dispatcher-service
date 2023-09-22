package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import java.nio.charset.StandardCharsets
import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

object QueueCommonsLogger {
  val logger: Logger = LoggerFactory.getLogger(QueueCommonsLogger::class.java)
}

fun writeEventToDeadLetterQueue(
  checkPointer: Checkpointer,
  eventPayload: ByteArray,
  exception: Throwable,
  deadLetterQueueAsyncClient: QueueAsyncClient,
  deadLetterQueueTTLSeconds: Int
): Mono<Void> {
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
      deadLetterQueueAsyncClient
        .sendMessageWithResponse(
          binaryData,
          Duration.ZERO,
          Duration.ofSeconds(deadLetterQueueTTLSeconds.toLong()), // timeToLive
        )
        .doOnNext {
          QueueCommonsLogger.logger.info(
            "Event: [${eventPayload.toString(StandardCharsets.UTF_8)}] successfully sent with visibility timeout: [${it.value.timeNextVisible}] ms to queue: [${deadLetterQueueAsyncClient.queueName}]")
        }
        .doOnError { queueException ->
          QueueCommonsLogger.logger.error(
            "Error sending event: [${eventPayload.toString(StandardCharsets.UTF_8)}] to dead letter queue.",
            queueException)
        }
        .then())
}
