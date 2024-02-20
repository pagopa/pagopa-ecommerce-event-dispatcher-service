package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedRetriedEvent
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for transactions for which retrieve authorization outcome status. This consumer
 * will handle TransactionAuthorizationRequestedEvent V2 events and inquiry gateway in order to
 * retrieve authorization outcome
 */
@Service("TransactionAuthorizationRequestedRetryQueueConsumer")
class TransactionAuthorizationRequestedRetryQueueConsumer(
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionAuthorizationRequestedRetryQueueConsumer,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  var logger: Logger =
    LoggerFactory.getLogger(TransactionAuthorizationRequestedRetryQueueConsumer::class.java)

  fun parseEvent(payload: ByteArray): Mono<QueueEvent<TransactionAuthorizationRequestedRetriedEvent>> {
    val data = BinaryData.fromBytes(payload)
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()

    return data
      .toObjectAsync(
        object : TypeReference<QueueEvent<TransactionAuthorizationRequestedRetriedEvent>>() {},
        jsonSerializerV2)
      .onErrorMap {
        logger.debug(ERROR_PARSING_EVENT_ERROR, it)
        InvalidEventException(data.toBytes(), it)
      }
  }

  @ServiceActivator(
    inputChannel = "transactionsauthorizationrequestedretrychannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Unit> {
    val eventWithTracingInfo = parseEvent(payload)
    return eventWithTracingInfo
      .flatMap {
        logger.debug(
          "Event {} with tracing info {} dispatched to V2 handler", it.event, it.tracingInfo)
        queueConsumerV2.messageReceiver(it, checkPointer)
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
