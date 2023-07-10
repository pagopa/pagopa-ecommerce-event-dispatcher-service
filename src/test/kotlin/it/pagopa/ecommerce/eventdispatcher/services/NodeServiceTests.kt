package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.documents.v1.Transaction
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.utils.EuroUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.AdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.UserDto
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
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

      val activatedEvent = transactionActivateEvent()
      val canceledEvent = transactionUserCanceledEvent()
      val events = listOf(activatedEvent, canceledEvent) as List<TransactionEvent<Any>>
      val transactionId = activatedEvent.transactionId

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      println("ressss" + closePaymentRequestCaptor.value)
      assertEquals(
        "Annullato",
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
    }

  @Test
  fun `closePayment returns successfully for retry close payment on user cancel request transaction`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val activatedEvent = transactionActivateEvent()
      val canceledEvent = transactionUserCanceledEvent()
      val closureError = transactionClosureErrorEvent()

      val events =
        listOf(activatedEvent, canceledEvent, closureError) as List<TransactionEvent<Any>>
      val transactionId = activatedEvent.transactionId

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))
      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      assertEquals(
        "Annullato",
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
    }

  @Test
  fun `closePayment throws BadTransactionStatusException for only transaction activated event `() =
    runTest {
      val transactionId = TRANSACTION_ID
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent() as TransactionEvent<Any>
      val events = listOf(activatedEvent)
      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      /* test */

      assertThrows<BadTransactionStatusException> {
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome)
      }
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly`() =
    runTest {
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()
      val authCompletedEvent = transactionAuthorizationCompletedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureError)
          as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      val additionalPaymentInfo = closePaymentRequestCaptor.value.additionalPaymentInformations!!
      val expectedTimestamp =
        OffsetDateTime.parse(
            authCompletedEvent.data.timestampOperation, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .truncatedTo(ChronoUnit.SECONDS)
          .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
      val expectedFee = EuroUtils.euroCentsToEuro(authEvent.data.fee).toString()
      val expectedTotalAmount =
        EuroUtils.euroCentsToEuro(authEvent.data.amount + authEvent.data.fee).toString()
      val expectedOutcome =
        authCompletedEvent.data.authorizationResultDto.let {
          OutcomePaymentGatewayEnum.valueOf(it.toString())
        }
      // Check Transaction Details
      assertEquals(
        "Autorizzato",
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(
        BigDecimal(authEvent.data.fee),
        closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertEquals(
        BigDecimal(authEvent.data.amount),
        closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        BigDecimal(authEvent.data.fee).add(BigDecimal(authEvent.data.amount)),
        closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertEquals(
        authCompletedEvent.data.authorizationCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.authorizationCode)
      assertEquals(
        ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime(),
        closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
      assertEquals(
        authEvent.data.pspId,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idPsp)
      assertEquals(
        authEvent.data.pspChannelCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
      assertEquals(
        authEvent.data.pspBusinessName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        authEvent.data.logo.toString(),
        closePaymentRequestCaptor.value.transactionDetails.info.brandLogo)

      // Check additionalPaymentInfo
      assertEquals(expectedTimestamp, additionalPaymentInfo.timestampOperation)
      // check that timestampOperation is in yyyy-MM-ddThh:mm:ss format
      assertTrue(
        additionalPaymentInfo.timestampOperation.matches(
          Regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$")))
      assertEquals(RRN, additionalPaymentInfo.rrn)
      assertEquals(expectedFee, additionalPaymentInfo.fee)
      assertEquals(expectedTotalAmount, additionalPaymentInfo.totalAmount)
      assertEquals(
        authCompletedEvent.data.authorizationCode, additionalPaymentInfo.authorizationCode)
      assertEquals(expectedOutcome, additionalPaymentInfo.outcomePaymentGateway)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO has not additional properties and transaction details valued correctly`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val authKO = AuthorizationResultDto.KO

      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()
      val authCompletedEvent = transactionAuthorizationCompletedEvent(authKO)
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureError)
          as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      // Check Transaction Details
      assertEquals(
        "Rifiutato",
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(
        ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime(),
        closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)

      // Check additionalPaymentInfo
      println("eccolo" + closePaymentRequestCaptor.value)
      assertNull(closePaymentRequestCaptor.value.additionalPaymentInformations)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for user cancellation has not additional properties and transaction details valued correctly`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val authKO = AuthorizationResultDto.KO

      val activatedEvent = transactionActivateEvent()
      val userCancellationRequested = transactionUserCanceledEvent()

      val transactionId = activatedEvent.transactionId
      val events = listOf(activatedEvent, userCancellationRequested) as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      /* preconditions */
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      // Check Transaction Details
      assertEquals(
        "Annullato",
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(
        ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime(),
        closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals("CP", closePaymentRequestCaptor.value.transactionDetails.info.type)

      // Check additionalPaymentInfo
      assertNull(closePaymentRequestCaptor.value.additionalPaymentInformations)
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
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
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
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())

      given(nodeClient.closePayment(any())).willReturn(Mono.just(closePaymentResponse))

      /* test */
      assertThrows<BadTransactionStatusException> {
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome)
      }
    }
}
