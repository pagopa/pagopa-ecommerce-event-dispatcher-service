package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.authorization.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.utils.EuroUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
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
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
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
      val amount =
        BigDecimal(activatedEvent.data.paymentNotices.stream().mapToInt { el -> el.amount }.sum())
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

      assertEquals(transactionId, closePaymentRequestCaptor.value.transactionId)
      assertEquals(OutcomeEnum.KO, closePaymentRequestCaptor.value.outcome)
      // check additionalPaymentInformations
      assertNull(closePaymentRequestCaptor.value.additionalPaymentInformations)
      // check transactionDetails
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CANCELED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(TIPO_VERSAMENTO_CP, closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        closePaymentRequestCaptor.value.transactionDetails.transaction.amount,
        closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertEquals(amount, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        amount, closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
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
      val amount =
        BigDecimal(activatedEvent.data.paymentNotices.stream().mapToInt { el -> el.amount }.sum())

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

      assertEquals(transactionId, closePaymentRequestCaptor.value.transactionId)
      assertEquals(OutcomeEnum.KO, closePaymentRequestCaptor.value.outcome)
      // check additionalPaymentInformations
      assertNull(closePaymentRequestCaptor.value.additionalPaymentInformations)
      // check transactionDetails
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CANCELED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(TIPO_VERSAMENTO_CP, closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        closePaymentRequestCaptor.value.transactionDetails.transaction.amount,
        closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertEquals(amount, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        amount, closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
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

  companion object {
    @JvmStatic
    private fun closePaymentDateFormat() =
      Stream.of(
        Arguments.of("2023-05-01T23:59:59.000Z", "2023-05-02T01:59:59"),
        Arguments.of("2023-12-01T23:59:59.000Z", "2023-12-02T00:59:59"))
  }

  @ParameterizedTest
  @MethodSource("closePaymentDateFormat")
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly`(
    timestampOperation: String,
    expectedLocalDate: String
  ) = runTest {
    val transactionOutcome = OutcomeEnum.OK

    val activatedEvent = transactionActivateEvent()
    val authEvent = transactionAuthorizationRequestedEvent()
    val authCompletedEvent =
      transactionAuthorizationCompletedEvent(
        pgsTransactionGatewayAuthorizationData(AuthorizationResultDto.OK))
    val closureRequestedEvent = transactionClosureRequestedEvent()
    val closureError = transactionClosureErrorEvent()
    val transactionId = activatedEvent.transactionId
    val nodoTimestampOperation = OffsetDateTime.parse(timestampOperation)
    authCompletedEvent.data.timestampOperation = nodoTimestampOperation.toString()
    val events =
      listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
        as List<TransactionEvent<Any>>

    val closePaymentResponse =
      ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

    val fee = authEvent.data.fee
    val amount = authEvent.data.amount
    val totalAmount = amount + fee

    val feeEuro = EuroUtils.euroCentsToEuro(fee)
    val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

    val feeEuroCents = BigDecimal(fee)
    val amountEuroCents = BigDecimal(amount)
    val totalAmountEuroCents = BigDecimal(totalAmount)

    val expectedFee = feeEuro.toString()
    val expectedTotalAmount = totalAmountEuro.toString()

    /* preconditions */
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())

    given(nodeClient.closePayment(capture(closePaymentRequestCaptor)))
      .willReturn(Mono.just(closePaymentResponse))

    /* test */
    assertEquals(
      closePaymentResponse,
      nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
    val additionalPaymentInfo = closePaymentRequestCaptor.value.additionalPaymentInformations!!

    val expectedOutcome =
      authCompletedEvent.data.transactionGatewayAuthorizationData.let {
        when (it) {
          is PgsTransactionGatewayAuthorizationData ->
            OutcomePaymentGatewayEnum.valueOf(it.authorizationResultDto.toString())
          is NpgTransactionGatewayAuthorizationData ->
            if (it.operationResult == OperationResultDto.EXECUTED) {
              OutcomePaymentGatewayEnum.OK
            } else {
              OutcomePaymentGatewayEnum.KO
            }
          is RedirectTransactionGatewayAuthorizationData -> TODO()
        }
      }
    // Check Transaction Details
    assertEquals(
      TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status,
      closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
    assertEquals(
      transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
    assertEquals(feeEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
    assertEquals(
      amountEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
    assertEquals(
      totalAmountEuroCents,
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
      authEvent.data.brokerName,
      closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.brokerName)
    assertEquals(
      authEvent.data.pspChannelCode,
      closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
    assertEquals(
      authEvent.data.pspBusinessName,
      closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
    assertEquals(
      authEvent.data.isPspOnUs,
      closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.pspOnUs)
    assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.errorCode)
    assertNotNull(closePaymentRequestCaptor.value.transactionDetails.transaction.timestampOperation)
    assertEquals(
      authEvent.data.paymentGateway.name,
      closePaymentRequestCaptor.value.transactionDetails.transaction.paymentGateway)
    assertEquals(
      UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
    assertEquals(
      Transaction.ClientId.CHECKOUT.name,
      closePaymentRequestCaptor.value.transactionDetails.info.clientId)
    assertEquals(
      authEvent.data.paymentTypeCode, closePaymentRequestCaptor.value.transactionDetails.info.type)
    assertEquals(
      (authEvent.data.transactionGatewayAuthorizationRequestedData
          as PgsTransactionGatewayAuthorizationRequestedData)
        .brand!!
        .name,
      closePaymentRequestCaptor.value.transactionDetails.info.brand)
    assertEquals(
      authEvent.data.paymentMethodName,
      closePaymentRequestCaptor.value.transactionDetails.info.paymentMethodName)
    assertEquals(
      (authEvent.data.transactionGatewayAuthorizationRequestedData
          as PgsTransactionGatewayAuthorizationRequestedData)
        .logo
        .toString(),
      closePaymentRequestCaptor.value.transactionDetails.info.brandLogo)
    // Check additionalPaymentInfo
    assertEquals(expectedLocalDate, additionalPaymentInfo.timestampOperation)
    // check that timestampOperation is in yyyy-MM-ddThh:mm:ss format
    assertTrue(
      additionalPaymentInfo.timestampOperation.matches(
        Regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$")))
    assertEquals(RRN, additionalPaymentInfo.rrn)
    assertEquals(expectedFee, additionalPaymentInfo.fee)
    assertEquals(expectedTotalAmount, additionalPaymentInfo.totalAmount)
    assertEquals(authCompletedEvent.data.authorizationCode, additionalPaymentInfo.authorizationCode)
    assertEquals(expectedOutcome, additionalPaymentInfo.outcomePaymentGateway)
    assertEquals(feeEuro, closePaymentRequestCaptor.value.fee)
    assertEquals(totalAmountEuro, closePaymentRequestCaptor.value.totalAmount)
  }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for NPG payment gateway`() =
    runTest {
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
          as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      val expectedFee = feeEuro.toString()
      val expectedTotalAmount = totalAmountEuro.toString()

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
      val expectedOutcome =
        authCompletedEvent.data.transactionGatewayAuthorizationData.let {
          when (it) {
            is PgsTransactionGatewayAuthorizationData ->
              OutcomePaymentGatewayEnum.valueOf(it.authorizationResultDto.toString())
            is NpgTransactionGatewayAuthorizationData ->
              if (it.operationResult == OperationResultDto.EXECUTED) {
                OutcomePaymentGatewayEnum.OK
              } else {
                OutcomePaymentGatewayEnum.KO
              }
            is RedirectTransactionGatewayAuthorizationData -> TODO()
          }
        }
      // Check Transaction Details
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(feeEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertEquals(
        amountEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        totalAmountEuroCents,
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
        authEvent.data.brokerName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.brokerName)
      assertEquals(
        authEvent.data.pspChannelCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
      assertEquals(
        authEvent.data.pspBusinessName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
      assertEquals(
        authEvent.data.isPspOnUs,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.pspOnUs)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.errorCode)
      assertNotNull(
        closePaymentRequestCaptor.value.transactionDetails.transaction.timestampOperation)
      assertEquals(
        authEvent.data.paymentGateway.name,
        closePaymentRequestCaptor.value.transactionDetails.transaction.paymentGateway)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as NpgTransactionGatewayAuthorizationRequestedData)
          .brand,
        closePaymentRequestCaptor.value.transactionDetails.info.brand)
      assertEquals(
        authEvent.data.paymentMethodName,
        closePaymentRequestCaptor.value.transactionDetails.info.paymentMethodName)
      assertEquals(
        authEvent.data.transactionGatewayAuthorizationRequestedData.logo.toString(),
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
      assertEquals(feeEuro, closePaymentRequestCaptor.value.fee)
      assertEquals(totalAmountEuro, closePaymentRequestCaptor.value.totalAmount)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK authorization KO has additional properties and transaction details valued correctly for PGS payment gateway`() =
    runTest {
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(
          pgsTransactionGatewayAuthorizationData(AuthorizationResultDto.KO))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
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

      val expectedOutcome =
        authCompletedEvent.data.transactionGatewayAuthorizationData.let {
          when (it) {
            is PgsTransactionGatewayAuthorizationData ->
              OutcomePaymentGatewayEnum.valueOf(it.authorizationResultDto.toString())
            is NpgTransactionGatewayAuthorizationData ->
              if (it.operationResult == OperationResultDto.EXECUTED) {
                OutcomePaymentGatewayEnum.OK
              } else {
                OutcomePaymentGatewayEnum.KO
              }
            is RedirectTransactionGatewayAuthorizationData -> TODO()
          }
        }

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      val expectedFee = feeEuro.toString()
      val expectedTotalAmount = totalAmountEuro.toString()

      // Check Transaction Details
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(feeEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertEquals(
        amountEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        totalAmountEuroCents,
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
        authEvent.data.brokerName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.brokerName)
      assertEquals(
        authEvent.data.pspChannelCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
      assertEquals(
        authEvent.data.pspBusinessName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
      assertEquals(
        authEvent.data.isPspOnUs,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.pspOnUs)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.errorCode)
      assertNotNull(
        closePaymentRequestCaptor.value.transactionDetails.transaction.timestampOperation)
      assertEquals(
        authEvent.data.paymentGateway.name,
        closePaymentRequestCaptor.value.transactionDetails.transaction.paymentGateway)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as PgsTransactionGatewayAuthorizationRequestedData)
          .brand!!
          .name,
        closePaymentRequestCaptor.value.transactionDetails.info.brand)
      assertEquals(
        authEvent.data.paymentMethodName,
        closePaymentRequestCaptor.value.transactionDetails.info.paymentMethodName)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as PgsTransactionGatewayAuthorizationRequestedData)
          .logo
          .toString(),
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

      assertEquals(feeEuro, closePaymentRequestCaptor.value.fee)
      assertEquals(totalAmountEuro, closePaymentRequestCaptor.value.totalAmount)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK authorization KO has additional properties and transaction details valued correctly for NPG payment gateway`() =
    runTest {
      val transactionOutcome = OutcomeEnum.OK

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
          as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      val expectedFee = feeEuro.toString()
      val expectedTotalAmount = totalAmountEuro.toString()

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

      val expectedOutcome =
        authCompletedEvent.data.transactionGatewayAuthorizationData.let {
          when (it) {
            is PgsTransactionGatewayAuthorizationData ->
              OutcomePaymentGatewayEnum.valueOf(it.authorizationResultDto.toString())
            is NpgTransactionGatewayAuthorizationData ->
              if (it.operationResult == OperationResultDto.EXECUTED) {
                OutcomePaymentGatewayEnum.OK
              } else {
                OutcomePaymentGatewayEnum.KO
              }
            is RedirectTransactionGatewayAuthorizationData -> TODO()
          }
        }
      // Check Transaction Details
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(feeEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertEquals(
        amountEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        totalAmountEuroCents,
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
        authEvent.data.brokerName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.brokerName)
      assertEquals(
        authEvent.data.pspChannelCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
      assertEquals(
        authEvent.data.pspBusinessName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
      assertEquals(
        authEvent.data.isPspOnUs,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.pspOnUs)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.errorCode)
      assertNotNull(
        closePaymentRequestCaptor.value.transactionDetails.transaction.timestampOperation)
      assertEquals(
        authEvent.data.paymentGateway.name,
        closePaymentRequestCaptor.value.transactionDetails.transaction.paymentGateway)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as NpgTransactionGatewayAuthorizationRequestedData)
          .brand,
        closePaymentRequestCaptor.value.transactionDetails.info.brand)
      assertEquals(
        authEvent.data.paymentMethodName,
        closePaymentRequestCaptor.value.transactionDetails.info.paymentMethodName)
      assertEquals(
        authEvent.data.transactionGatewayAuthorizationRequestedData.logo.toString(),
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
      assertEquals(feeEuro, closePaymentRequestCaptor.value.fee)
      assertEquals(totalAmountEuro, closePaymentRequestCaptor.value.totalAmount)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO has not additional properties and transaction details valued correctly for PGS gateway`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val authKO = AuthorizationResultDto.KO
      val errorCode = "errorCode"

      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData(errorCode, authKO))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
          as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

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

      // Check close payment request information
      assertNull(closePaymentRequestCaptor.value.fee)
      assertNull(closePaymentRequestCaptor.value.idChannel)
      assertNull(closePaymentRequestCaptor.value.idBrokerPSP)
      assertNull(closePaymentRequestCaptor.value.timestampOperation)
      assertNull(closePaymentRequestCaptor.value.paymentMethod)
      assertNull(closePaymentRequestCaptor.value.totalAmount)
      assertNull(closePaymentRequestCaptor.value.idPSP)
      // Check Transaction Details
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(feeEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertEquals(
        amountEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        totalAmountEuroCents,
        closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertEquals(
        errorCode, closePaymentRequestCaptor.value.transactionDetails.transaction.errorCode)
      assertEquals(
        ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime(),
        closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
      assertEquals(
        authEvent.data.pspId,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idPsp)
      assertEquals(
        authEvent.data.brokerName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.brokerName)
      assertEquals(
        authEvent.data.pspChannelCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
      assertEquals(
        authEvent.data.pspBusinessName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
      assertEquals(
        authEvent.data.isPspOnUs,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.pspOnUs)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.authorizationCode)
      assertNotNull(
        closePaymentRequestCaptor.value.transactionDetails.transaction.timestampOperation)
      assertEquals(
        authEvent.data.paymentGateway.name,
        closePaymentRequestCaptor.value.transactionDetails.transaction.paymentGateway)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as PgsTransactionGatewayAuthorizationRequestedData)
          .brand!!
          .name,
        closePaymentRequestCaptor.value.transactionDetails.info.brand)
      assertEquals(
        authEvent.data.paymentMethodName,
        closePaymentRequestCaptor.value.transactionDetails.info.paymentMethodName)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as PgsTransactionGatewayAuthorizationRequestedData)
          .logo
          .toString(),
        closePaymentRequestCaptor.value.transactionDetails.info.brandLogo)

      // Check additionalPaymentInfo
      assertNull(closePaymentRequestCaptor.value.additionalPaymentInformations)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO has not additional properties and transaction details valued correctly for NPG gateway`() =
    runTest {
      val transactionOutcome = OutcomeEnum.KO

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
          as List<TransactionEvent<Any>>

      val closePaymentResponse =
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK }

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

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

      // Check close payment request information
      assertNull(closePaymentRequestCaptor.value.fee)
      assertNull(closePaymentRequestCaptor.value.idChannel)
      assertNull(closePaymentRequestCaptor.value.idBrokerPSP)
      assertNull(closePaymentRequestCaptor.value.timestampOperation)
      assertNull(closePaymentRequestCaptor.value.paymentMethod)
      assertNull(closePaymentRequestCaptor.value.totalAmount)
      assertNull(closePaymentRequestCaptor.value.idPSP)
      // Check Transaction Details
      assertEquals(
        TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status,
        closePaymentRequestCaptor.value.transactionDetails.transaction.transactionStatus)
      assertEquals(
        transactionId, closePaymentRequestCaptor.value.transactionDetails.transaction.transactionId)
      assertEquals(feeEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.fee)
      assertEquals(
        amountEuroCents, closePaymentRequestCaptor.value.transactionDetails.transaction.amount)
      assertEquals(
        totalAmountEuroCents,
        closePaymentRequestCaptor.value.transactionDetails.transaction.grandTotal)
      assertEquals(null, closePaymentRequestCaptor.value.transactionDetails.transaction.errorCode)
      assertEquals(
        ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime(),
        closePaymentRequestCaptor.value.transactionDetails.transaction.creationDate)
      assertEquals(
        authEvent.data.pspId,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idPsp)
      assertEquals(
        authEvent.data.brokerName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.brokerName)
      assertEquals(
        authEvent.data.pspChannelCode,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.idChannel)
      assertEquals(
        authEvent.data.pspBusinessName,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.businessName)
      assertEquals(
        authEvent.data.isPspOnUs,
        closePaymentRequestCaptor.value.transactionDetails.transaction.psp!!.pspOnUs)
      assertNull(closePaymentRequestCaptor.value.transactionDetails.transaction.authorizationCode)
      assertNotNull(
        closePaymentRequestCaptor.value.transactionDetails.transaction.timestampOperation)
      assertEquals(
        authEvent.data.paymentGateway.name,
        closePaymentRequestCaptor.value.transactionDetails.transaction.paymentGateway)
      assertEquals(
        UserDto.TypeEnum.GUEST, closePaymentRequestCaptor.value.transactionDetails.user.type)
      assertEquals(
        Transaction.ClientId.CHECKOUT.name,
        closePaymentRequestCaptor.value.transactionDetails.info.clientId)
      assertEquals(
        authEvent.data.paymentTypeCode,
        closePaymentRequestCaptor.value.transactionDetails.info.type)
      assertEquals(
        (authEvent.data.transactionGatewayAuthorizationRequestedData
            as NpgTransactionGatewayAuthorizationRequestedData)
          .brand,
        closePaymentRequestCaptor.value.transactionDetails.info.brand)
      assertEquals(
        authEvent.data.paymentMethodName,
        closePaymentRequestCaptor.value.transactionDetails.info.paymentMethodName)
      assertEquals(
        authEvent.data.transactionGatewayAuthorizationRequestedData.logo.toString(),
        closePaymentRequestCaptor.value.transactionDetails.info.brandLogo)

      // Check additionalPaymentInfo
      assertNull(closePaymentRequestCaptor.value.additionalPaymentInformations)
    }

  @Test
  fun `closePayment returns successfully for close payment after authorization Completed from NPG KO`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEndTOEndId"))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
          as List<TransactionEvent<Any>>

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
      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()

      val transactionId = activatedEvent.transactionId
      val events = listOf(activatedEvent, authEvent) as List<TransactionEvent<Any>>
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
