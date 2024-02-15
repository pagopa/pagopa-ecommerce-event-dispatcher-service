package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent as TransactionAuthorizationRequestedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for transactions to refund. These events are input in the event queue only when a
 * transaction is stuck in an REFUND_REQUESTED state **and** needs to be reverted
 */
@Service("TransactionAuthorizationRequestedQueueConsumer")
class TransactionAuthorizationRequestedQueueConsumer(
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionAuthorizationRequestedQueueConsumer,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsRefundQueueConsumer::class.java)

  fun parseEvent(
    payload: ByteArray
  ): Mono<Pair<TransactionAuthorizationRequestedEventV2, TracingInfo?>> {
    val data = BinaryData.fromBytes(payload)
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()

    val transactionAuthorizationRequestedEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEventV2>>() {},
          jsonSerializerV2)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    return transactionAuthorizationRequestedEventV2.onErrorMap(NoSuchElementException::class.java) {
      InvalidEventException(data.toBytes(), it)
    }
  }

  @ServiceActivator(
    inputChannel = "transactionsauthorizationrequestedchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Unit> {
    val eventWithTracingInfo = parseEvent(payload)
    return eventWithTracingInfo
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionAuthorizationRequestedEventV2 -> {
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
          checkPointer,
          payload,
          it,
          deadLetterTracedQueueAsyncClient,
          DeadLetterTracedQueueAsyncClient.PARSING_EVENT_ERROR_CONTEXT)
      }
  }
}
