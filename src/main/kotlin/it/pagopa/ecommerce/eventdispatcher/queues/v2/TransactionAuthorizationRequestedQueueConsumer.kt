package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedEvent
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
    parsedEvent:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val authorizationRequestedEvent =
      parsedEvent.fold(
        { AuthorizationRequestedEvent.requested(it) }, { AuthorizationRequestedEvent.retried(it) })
    return authorizationRequestedHelper.authorizationStateRetrieve(
      authorizationRequestedEvent, checkPointer)
  }
}
