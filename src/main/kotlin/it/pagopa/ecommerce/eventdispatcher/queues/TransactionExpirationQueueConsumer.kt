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
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
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

/**
 * Event consumer for events related to transaction activation. This consumer's responsibilities are
 * to handle expiration of transactions and subsequent refund for transaction stuck in a
 * pending/transient state.
 */
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
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  val logger: Logger = LoggerFactory.getLogger(TransactionExpirationQueueConsumer::class.java)

  fun parseEvent(data: BinaryData): Mono<Pair<BaseTransactionEvent<*>, TracingInfo?>> {
    val jsonSerializerV1 = strictSerializerProviderV1.createInstance()
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()
    val transactionActivatedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionActivatedEventV1>>() {}, jsonSerializerV1)
        .map { it.event to it.tracingInfo }
        .doOnNext { logger.info("{} event dispatched to V1 handler", it) }

    val transactionExpiredEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionExpiredEventV1>>() {}, jsonSerializerV1)
        .map { it.event to it.tracingInfo }
        .doOnNext { logger.info("{} event dispatched to V1 handler", it) }

    val untracedTransactionActivatedEventV1 =
      data
        .toObjectAsync(object : TypeReference<TransactionActivatedEventV1>() {}, jsonSerializerV1)
        .map { it to null }
        .doOnNext { logger.info("{} event dispatched to V1 handler", it) }

    val untracedTransactionExpiredEventV1 =
      data
        .toObjectAsync(object : TypeReference<TransactionExpiredEventV1>() {}, jsonSerializerV1)
        .map { it to null }
        .doOnNext { logger.info("{} event dispatched to V1 handler", it) }

    val transactionActivatedEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionActivatedEventV2>>() {}, jsonSerializerV2)
        .map { it.event to it.tracingInfo }

    val transactionExpiredEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionExpiredEventV2>>() {}, jsonSerializerV2)
        .map { it.event to it.tracingInfo }

    return Mono.firstWithValue(
        transactionActivatedEventV1,
        transactionExpiredEventV1,
        untracedTransactionActivatedEventV1,
        untracedTransactionExpiredEventV1,
        transactionActivatedEventV2,
        transactionExpiredEventV2)
      .onErrorMap { InvalidEventException(data.toBytes(), it) }
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
            logger.info("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(
              Pair(Either.left(e), tracingInfo), checkPointer, headers)
          }
          is TransactionExpiredEventV1 -> {
            logger.info("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(
              Pair(Either.right(e), tracingInfo), checkPointer, headers)
          }
          is TransactionActivatedEventV2 -> {
            logger.info("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(
              Either.left(QueueEvent(e, tracingInfo)), checkPointer, headers)
          }
          is TransactionExpiredEventV2 -> {
            logger.info("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(
              Either.right(QueueEvent(e, tracingInfo)), checkPointer, headers)
          }
          else -> {
            logger.error(
              "Event {} with tracing info {} cannot be dispatched to any know handler",
              e,
              tracingInfo)
            Mono.error(InvalidEventException(payload, null)) // FIXME
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
