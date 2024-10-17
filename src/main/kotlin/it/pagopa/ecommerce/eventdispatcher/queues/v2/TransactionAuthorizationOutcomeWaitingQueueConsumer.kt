package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationOutcomeWaitingEvent
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
    return authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
      parsedEvent, checkPointer)
  }
}
