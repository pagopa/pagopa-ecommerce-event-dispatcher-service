package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedHelper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to refund retry. This consumer's responsibilities are to handle
 * refund process retry for a given transaction
 */
@Service("TransactionAuthorizationRequestedQueueConsumerV2")
class TransactionAuthorizationRequestedQueueConsumer(
  @Autowired private val authorizationRequestedHelper: AuthorizationRequestedHelper,
) {

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionAuthorizationRequestedEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    return authorizationRequestedHelper
      .authorizationRequestedHandler(parsedEvent, checkPointer)
      .contextWrite { context ->
        LogTracingUtils.enrichContextForEvent(
          mapOf(
            LogTracingUtils.TracingEntry.CTX_TRANSACTION_ID to parsedEvent.event.transactionId,
            LogTracingUtils.TracingEntry.CTX_EVENT_CODE to parsedEvent.event.eventCode,
            LogTracingUtils.TracingEntry.CTX_EVENT_ID to parsedEvent.event.id,
            LogTracingUtils.TracingEntry.EVENT_ACTION to "AUTHORIZATION_REQUESTED"),
          context)
      }
  }
}
