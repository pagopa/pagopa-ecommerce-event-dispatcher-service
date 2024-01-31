package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentEvent
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentHelper
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class TransactionClosePaymentQueueConsumerTests {

  private val closePaymentHelper: ClosePaymentHelper = mock()

  private val checkpointer: Checkpointer = mock()

  private val transactionClosureEventsConsumer =
    TransactionClosePaymentQueueConsumer(closePaymentHelper = closePaymentHelper)

  @Test
  fun `Should handle close payment for user cancel event`() {
    // assertions
    val event = QueueEvent(transactionUserCanceledEvent(), null)
    val expectedClosePaymentEvent = ClosePaymentEvent.canceled(event)
    given(closePaymentHelper.closePayment(any(), any(), any())).willReturn(mono { (Unit) })
    // test
    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(Either.left(event), checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    verify(closePaymentHelper, times(1))
      .closePayment(eq(expectedClosePaymentEvent), eq(checkpointer), any())
  }

  @Test
  fun `Should handle close payment for closure requested event`() {
    // assertions
    val event = QueueEvent(transactionClosureRequestedEvent(), null)
    val expectedClosePaymentEvent = ClosePaymentEvent.requested(event)
    given(closePaymentHelper.closePayment(any(), any(), any())).willReturn(mono { (Unit) })
    // test
    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(Either.right(event), checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    verify(closePaymentHelper, times(1))
      .closePayment(eq(expectedClosePaymentEvent), eq(checkpointer), any())
  }
}
