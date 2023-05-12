package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.*
import org.mockito.BDDMockito.given
import org.mockito.kotlin.any
import org.mockito.kotlin.capture
import org.springframework.test.context.junit.jupiter.SpringExtension
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

@ExtendWith(SpringExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class NodeServiceTests {

  @InjectMocks lateinit var nodeService: NodeService

  @Mock lateinit var nodeClient: NodeClient

  @Mock lateinit var transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>

  @Captor private lateinit var closePaymentRequestCaptor: ArgumentCaptor<ClosePaymentRequestV2Dto>

  @Test
  fun `closePayment returns successfully for close payment on user cancel request transaction`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
      val canceledEvent = transactionUserCanceledEvent() as TransactionEvent<Any>
      val events = listOf(activatedEvent, canceledEvent)
      val transactionId = activatedEvent.transactionId

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(transactionsEventStoreRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
    }

  @Test
  fun `closePayment returns successfully for retry close payment on user cancel request transaction`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
      val canceledEvent = transactionUserCanceledEvent() as TransactionEvent<Any>
      val closureError = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events = listOf(activatedEvent, canceledEvent, closureError)
      val transactionId = activatedEvent.transactionId

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(transactionsEventStoreRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
    }

  @Test
  fun `closePayment throws BadTransactionStatusException for only transaction activated event `() =
    runTest {
      val transactionId = TRANSACTION_ID
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
      val events = listOf(activatedEvent)
      /* preconditions */
      given(transactionsEventStoreRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(events.toFlux())

      /* test */

      assertThrows<BadTransactionStatusException> {
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome)
      }
    }

  @Test
  fun `ClosePaymentRequestV2Dto has additional properties valued correctly`() = runTest {
    val transactionOutcome = OutcomeEnum.OK

    val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authEvent = transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authCompletedEvent = transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closureError = transactionClosureErrorEvent() as TransactionEvent<Any>
    val transactionId = activatedEvent.transactionId
    val events = listOf(activatedEvent, authEvent, authCompletedEvent, closureError)

    val closePaymentResponse =
      ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

    /* preconditions */
    given(transactionsEventStoreRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(events.toFlux())

    given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
      .willReturn(Mono.just(closePaymentResponse))

    /* test */
    assertEquals(
      closePaymentResponse,
      nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
    val closePaymentRequestV2Dto = closePaymentRequestCaptor.value
    val expectedTimestamp = closePaymentRequestV2Dto.timestampOperation
    assertEquals(
      expectedTimestamp,
      closePaymentRequestV2Dto.additionalPaymentInformations!!.timestampOperation)
  }

  @Test
  fun `closePayment returns successfully for close payment after authorization Completed from PGS KO`() =
    runTest {
      val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authEvent = transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>
      val closureError = transactionClosureErrorEvent() as TransactionEvent<Any>
      val transactionId = activatedEvent.transactionId
      val events = listOf(activatedEvent, authEvent, authCompletedEvent, closureError)

      val pgsOutCome = OutcomeEnum.KO

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(transactionsEventStoreRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse, nodeService.closePayment(TransactionId(transactionId), pgsOutCome))
    }

  @Test
  fun `closePayment returns error for close payment missing authorization completed event`() =
    runTest {
      val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authEvent = transactionAuthorizationRequestedEvent() as TransactionEvent<Any>

      val transactionId = activatedEvent.transactionId
      val events = listOf(activatedEvent, authEvent)
      val transactionOutcome = OutcomeEnum.OK

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(transactionsEventStoreRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertThrows<BadTransactionStatusException> {
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome)
      }
    }
}
