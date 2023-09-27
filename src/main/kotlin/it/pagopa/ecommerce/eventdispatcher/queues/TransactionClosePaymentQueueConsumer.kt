package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserCanceledEvent as TransactionUserCanceledEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserCanceledEvent as TransactionUserCanceledEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionClosePaymentQueueConsumer")
class TransactionClosePaymentQueueConsumer(
  @Autowired
  private val queueConsumerV1:
    it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionClosePaymentQueueConsumer,
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionClosePaymentQueueConsumer,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

  private fun parseEvent(
    binaryData: BinaryData
  ): Mono<Pair<BaseTransactionEvent<Void>, TracingInfo?>> {
    val jsonSerializerV1 = strictSerializerProviderV1.createInstance()
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()
    val queueEventV1 =
      binaryData
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserCanceledEventV1>>() {}, jsonSerializerV1)
        .map { Pair(it.event, it.tracingInfo) }

    val untracedEventV1 =
      binaryData
        .toObjectAsync(
          object : TypeReference<TransactionUserCanceledEventV1>() {}, jsonSerializerV1)
        .map { Pair(it, null) }
    val queueEventV2 =
      binaryData
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserCanceledEventV2>>() {}, jsonSerializerV2)
        .map { Pair(it.event, it.tracingInfo) }

    return Mono.firstWithValue(queueEventV1, untracedEventV1, queueEventV2).onErrorMap {
      InvalidEventException(binaryData.toBytes(), it)
    }
  }

  @ServiceActivator(inputChannel = "transactionclosureschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer,
  ): Mono<Void> {
    val parsedEvents = parseEvent(BinaryData.fromBytes(payload))
    return parsedEvents
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionUserCanceledEventV1 -> {
            logger.debug("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(e to tracingInfo, checkPointer)
          }
          is TransactionUserCanceledEventV2 -> {
            logger.debug("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(QueueEvent(e, tracingInfo), checkPointer)
          }
          else -> {
            logger.error(
              "Event {} with tracing info {} cannot be dispatched to any know handler",
              e,
              tracingInfo)
            Mono.error(InvalidEventException(payload, null))
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
