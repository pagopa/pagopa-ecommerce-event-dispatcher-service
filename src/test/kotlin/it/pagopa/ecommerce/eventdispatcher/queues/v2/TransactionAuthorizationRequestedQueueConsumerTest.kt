package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.AuthorizationRequestedHelper
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class TransactionAuthorizationRequestedQueueConsumerTest {

  private val authorizationRequestedHelper: AuthorizationRequestedHelper = mock()

  private val checkpointer: Checkpointer = mock()

  private val transactionAuthorizationRequestedQueueConsumer =
    TransactionAuthorizationRequestedQueueConsumer(
      authorizationRequestedHelper = authorizationRequestedHelper)

  @Test
  fun `Should handle authorization state retriever for authorization requested event`() {
    // assertions
    val event = QueueEvent(transactionAuthorizationRequestedEvent(), MOCK_TRACING_INFO)
    given(authorizationRequestedHelper.authorizationStateRetrieve(eq(Either.left(event)), any()))
      .willReturn(mono { (Unit) })
    // test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(event, checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    verify(authorizationRequestedHelper, times(1))
      .authorizationStateRetrieve(eq(Either.left(event)), eq(checkpointer))
  }
}
