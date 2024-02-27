package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedHelper
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class TransactionAuthorizationOutcomeWaitingQueueConsumerTest {

  private val authorizationRequestedHelper: AuthorizationRequestedHelper = mock()

  private val checkpointer: Checkpointer = mock()
  private val transactionAuthorizationOutcomeWaitingQueueConsumer =
    TransactionAuthorizationOutcomeWaitingQueueConsumer(
      authorizationRequestedHelper = authorizationRequestedHelper)

  @Test
  fun `Should handle authorization state retriever for authorization requested retry event`() {
    // assertions
    val event = QueueEvent(transactionAuthorizationOutcomeWaitingEvent(0), MOCK_TRACING_INFO)
    val expectedAuthorizationRequestedRetriedEvent = AuthorizationRequestedEvent.retried(event)
    given(
        authorizationRequestedHelper.authorizationStateRetrieve(
          eq(expectedAuthorizationRequestedRetriedEvent), any()))
      .willReturn(mono { (Unit) })
    // test
    StepVerifier.create(
        transactionAuthorizationOutcomeWaitingQueueConsumer.messageReceiver(
          Either.right(event), checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    verify(authorizationRequestedHelper, times(1))
      .authorizationStateRetrieve(eq(expectedAuthorizationRequestedRetriedEvent), eq(checkpointer))
  }
}
