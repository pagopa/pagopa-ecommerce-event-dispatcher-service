package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationOutcomeWaitingEvent
import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedHelper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionAuthorizationOutcomeWaitingQueueConsumerV2")
class TransactionAuthorizationOutcomeWaitingQueueConsumer(
  @Autowired private val authorizationRequestedHelper: AuthorizationRequestedHelper,
) {

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    return authorizationRequestedHelper
      .authorizationOutcomeWaitingHandler(parsedEvent, checkPointer)
      .contextWrite { context ->
        LogTracingUtils.enrichContextForEvent(
          mapOf(
            LogTracingUtils.AttributeKeys.CTX_TRANSACTION_ID to parsedEvent.event.transactionId,
            LogTracingUtils.AttributeKeys.CTX_EVENT_CODE to parsedEvent.event.eventCode,
            LogTracingUtils.AttributeKeys.CTX_EVENT_ID to parsedEvent.event.id,
            LogTracingUtils.AttributeKeys.EVENT_ACTION to "AUTHORIZATION_OUTCOME_WAITING"),
          context)
      }
  }
}
