package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionActivatedEvent as TransactionActivatedEventV1
import it.pagopa.ecommerce.commons.documents.v1.TransactionExpiredEvent as TransactionExpiredEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionActivatedEvent as TransactionActivatedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionExpiredEvent as TransactionExpiredEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionExpirationQueueConsumer as TransactionExpirationQueueConsumerV1
import it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionExpirationQueueConsumer as TransactionExpirationQueueConsumerV2
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Headers
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class TransactionExpirationQueueConsumer(
  @Autowired
  @Qualifier("TransactionExpirationQueueConsumerV1")
  private val queueConsumerV1: TransactionExpirationQueueConsumerV1,
  @Autowired
  @Qualifier("TransactionExpirationQueueConsumerV2")
  private val queueConsumerV2: TransactionExpirationQueueConsumerV2,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
) {

  val logger: Logger = LoggerFactory.getLogger(TransactionExpirationQueueConsumer::class.java)

  fun parseEvent(data: BinaryData): Mono<Pair<BaseTransactionEvent<*>, TracingInfo?>> {
    val transactionActivatedEventV1 =
      data.toObjectAsync(object : TypeReference<QueueEvent<TransactionActivatedEventV1>>() {}).map {
        it.event to it.tracingInfo
      }

    val transactionExpiredEventV1 =
      data.toObjectAsync(object : TypeReference<QueueEvent<TransactionExpiredEventV1>>() {}).map {
        it.event to it.tracingInfo
      }

    val untracedTransactionActivatedEventV1 =
      data.toObjectAsync(object : TypeReference<TransactionActivatedEventV1>() {}).map {
        it to null
      }

    val untracedTransactionExpiredEventV1 =
      data.toObjectAsync(object : TypeReference<TransactionExpiredEventV1>() {}).map { it to null }

    val transactionActivatedEventV2 =
      data.toObjectAsync(object : TypeReference<QueueEvent<TransactionActivatedEventV2>>() {}).map {
        it.event to it.tracingInfo
      }

    val transactionExpiredEventV2 =
      data.toObjectAsync(object : TypeReference<QueueEvent<TransactionExpiredEventV2>>() {}).map {
        it.event to it.tracingInfo
      }

    return Mono.firstWithValue(
        transactionActivatedEventV1,
        transactionExpiredEventV1,
        untracedTransactionActivatedEventV1,
        untracedTransactionExpiredEventV1,
        transactionActivatedEventV2,
        transactionExpiredEventV2)
      .onErrorMap { InvalidEventException(data.toBytes()) }
  }

  @ServiceActivator(inputChannel = "transactionexpiredchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer,
    @Headers headers: MessageHeaders
  ): Mono<Void> {
    val eventWithTracingInfo = parseEvent(BinaryData.fromBytes(payload))

    return eventWithTracingInfo
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionActivatedEventV1 -> {
            queueConsumerV1.messageReceiver(
              Pair(Either.left(e), tracingInfo), checkPointer, headers)
          }
          is TransactionExpiredEventV1 -> {
            queueConsumerV1.messageReceiver(
              Pair(Either.right(e), tracingInfo), checkPointer, headers)
          }
          is TransactionActivatedEventV2 -> {
            queueConsumerV2.messageReceiver(
              Either.left(QueueEvent(e, tracingInfo)), checkPointer, headers)
          }
          is TransactionExpiredEventV2 -> {
            queueConsumerV2.messageReceiver(
              Either.right(QueueEvent(e, tracingInfo)), checkPointer, headers)
          }
          else -> {
            Mono.error(InvalidEventException(payload)) // FIXME
          }
        }
      }
      .onErrorResume(InvalidEventException::class.java) {
        logger.error("Invalid input event", it)
        writeEventToDeadLetterQueue(
          checkPointer, payload, it, deadLetterQueueAsyncClient, deadLetterTTLSeconds)
      }
  }
}
