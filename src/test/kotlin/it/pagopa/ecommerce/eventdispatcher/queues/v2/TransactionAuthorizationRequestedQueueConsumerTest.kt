package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedHelper
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import reactor.test.StepVerifier

class TransactionAuthorizationRequestedQueueConsumerTest {

  private val authorizationRequestedHelper: AuthorizationRequestedHelper = mock()

  private val checkpointer: Checkpointer = mock()

  private val transactionAuthorizationRequestedQueueConsumer =
    TransactionAuthorizationRequestedQueueConsumer(
      authorizationRequestedHelper = authorizationRequestedHelper)

  @Test
  fun `Should handle authorization state retriever for authorization requested event`() {
    // assertions
    val event = QueueEvent(transactionAuthorizationRequestedEvent(), null)
    val expectedAuthorizationRequestedEvent = AuthorizationRequestedEvent.requested(event)
    given(authorizationRequestedHelper.authorizationStateRetrieve(any(), any()))
      .willReturn(mono { (Unit) })
    // test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          Either.left(event), checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    verify(authorizationRequestedHelper, times(1))
      .authorizationStateRetrieve(eq(expectedAuthorizationRequestedEvent), eq(checkpointer))
  }
}
