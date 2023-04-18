package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventNotFoundException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventsInconsistentException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionEventsPreconditionsNotMatchedException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
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

@ExtendWith(SpringExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class NodeServiceTests {

  @InjectMocks lateinit var nodeService: NodeService

  @Mock lateinit var nodeClient: NodeClient

  @Mock lateinit var transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>

  @Captor private lateinit var closePaymentRequestCaptor: ArgumentCaptor<ClosePaymentRequestV2Dto>

  @Test
  fun `closePayment returns successfully for close payment on authorization requested transaction`() =
    runTest {
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()

      val transactionId = activatedEvent.transactionId

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
        .willReturn(Mono.just(activatedEvent as TransactionEvent<Any>))

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
        .willReturn(Mono.empty())

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
        .willReturn(Mono.just(authEvent as TransactionEvent<Any>))

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(
          UUID.fromString(transactionId), transactionOutcome, Optional.of("authorizationCode")))
    }

  @Test
  fun `closePayment returns successfully for close payment on user cancel request transaction`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val activatedEvent = transactionActivateEvent()
      val canceledEvent = transactionUserCanceledEvent()

      val transactionId = activatedEvent.transactionId

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
        .willReturn(Mono.just(activatedEvent as TransactionEvent<Any>))

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
        .willReturn(Mono.just(canceledEvent as TransactionEvent<Any>))

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
        .willReturn(Mono.empty())

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(
          UUID.fromString(transactionId), transactionOutcome, Optional.empty()))
    }

  @Test
  fun `closePayment throws TransactionEventNotFoundException on transaction event not found`() =
    runTest {
      val transactionId = UUID.randomUUID()
      val transactionOutcome = OutcomeEnum.OK

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
        .willReturn(Mono.empty())

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
        .willReturn(Mono.empty())

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
        .willReturn(Mono.empty())

      /* test */

      assertThrows<TransactionEventNotFoundException> {
        nodeService.closePayment(
          transactionId, transactionOutcome, Optional.of("authorizationCode"))
      }
    }

  @Test
  fun `closePayment throws TransactionPreconditionsNotMatchedException on transaction event auth requested and user canceled request not found`() =
    runTest {
      val transactionId = UUID.randomUUID()
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
        .willReturn(Mono.just(activatedEvent as TransactionEvent<Any>))

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
        .willReturn(Mono.empty())

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
        .willReturn(Mono.empty())

      /* test */

      assertThrows<TransactionEventsPreconditionsNotMatchedException> {
        nodeService.closePayment(
          transactionId, transactionOutcome, Optional.of("authorizationCode"))
      }
    }

  @Test
  fun `closePayment throws TransactionEventsInconsistentException on transaction event auth requested and user canceled request both found`() =
    runTest {
      val transactionId = UUID.randomUUID()
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()
      val canceledEvent = transactionUserCanceledEvent()
      val authEvent = transactionAuthorizationRequestedEvent()

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
        .willReturn(Mono.just(activatedEvent as TransactionEvent<Any>))

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(),
            TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
        .willReturn(Mono.just(authEvent as TransactionEvent<Any>))

      given(
          transactionsEventStoreRepository.findByTransactionIdAndEventCode(
            transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
        .willReturn(Mono.just(canceledEvent as TransactionEvent<Any>))

      /* test */

      assertThrows<TransactionEventsInconsistentException> {
        nodeService.closePayment(
          transactionId, transactionOutcome, Optional.of("authorizationCode"))
      }
    }

  @Test
  fun `ClosePaymentRequestV2Dto has additional properties valued correctly`() = runTest {
    val transactionOutcome = OutcomeEnum.OK

    val activatedEvent = transactionActivateEvent()
    val authEvent = transactionAuthorizationRequestedEvent()

    val transactionId = activatedEvent.transactionId

    val closePaymentResponse =
      ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

    /* preconditions */
    given(
        transactionsEventStoreRepository.findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_ACTIVATED_EVENT))
      .willReturn(Mono.just(activatedEvent as TransactionEvent<Any>))

    given(
        transactionsEventStoreRepository.findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT))
      .willReturn(Mono.empty())

    given(
        transactionsEventStoreRepository.findByTransactionIdAndEventCode(
          transactionId.toString(), TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT))
      .willReturn(Mono.just(authEvent as TransactionEvent<Any>))

    given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
      .willReturn(Mono.just(closePaymentResponse))

    /* test */
    assertEquals(
      closePaymentResponse,
      nodeService.closePayment(
        UUID.fromString(transactionId), transactionOutcome, Optional.of("authorizationCode")))
    val closePaymentRequestV2Dto = closePaymentRequestCaptor.value
    val expectedTimestamp =
      closePaymentRequestV2Dto.timestampOperation!!
        .truncatedTo(ChronoUnit.SECONDS)!!
        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    assertEquals(
      expectedTimestamp,
      closePaymentRequestV2Dto.additionalPaymentInformations!!["timestampOperation"])
  }
}
