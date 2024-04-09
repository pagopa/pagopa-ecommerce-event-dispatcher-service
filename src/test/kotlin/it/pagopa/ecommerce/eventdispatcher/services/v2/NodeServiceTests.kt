package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.authorization.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.utils.EuroUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import it.pagopa.generated.ecommerce.nodo.v2.dto.CardAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum
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

  @Captor
  private lateinit var closePaymentRequestCaptor: ArgumentCaptor<CardClosePaymentRequestV2Dto>

  @Captor
  private lateinit var redirectClosePaymentRequestCaptor:
    ArgumentCaptor<RedirectClosePaymentRequestV2Dto>

  @Captor
  private lateinit var paypalClosePaymentRequestCaptor:
    ArgumentCaptor<PayPalClosePaymentRequestV2Dto>

  @Captor
  private lateinit var bancomatPayClosePaymentRequestCaptor:
    ArgumentCaptor<BancomatPayClosePaymentRequestV2Dto>

  @Test
  fun `closePayment returns successfully for close payment on user cancel request transaction`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO

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
      assertEquals(
        CardClosePaymentRequestV2Dto.OutcomeEnum.KO, closePaymentRequestCaptor.value.outcome)
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
      val transactionOutcome = ClosePaymentOutcome.KO

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
      assertEquals(
        CardClosePaymentRequestV2Dto.OutcomeEnum.KO, closePaymentRequestCaptor.value.outcome)
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
      val transactionOutcome = ClosePaymentOutcome.OK

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
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for PGS payment gateway`(
    timestampOperation: String,
    expectedLocalDate: String
  ) = runTest {
    val transactionOutcome = ClosePaymentOutcome.OK

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
          is RedirectTransactionGatewayAuthorizationData ->
            if (it.outcome == RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
              OutcomePaymentGatewayEnum.OK
            } else {
              OutcomePaymentGatewayEnum.KO
            }
        }
      }

    // Check close payment request information
    val expected =
      CardClosePaymentRequestV2Dto().apply {
        outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.OK
        this.transactionId = transactionId
        paymentTokens =
          activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
        this.timestampOperation = OffsetDateTime.parse(authCompletedEvent.data.timestampOperation)
        this.fee = feeEuro
        idPSP = authEvent.data.pspId
        idChannel = authEvent.data.pspChannelCode
        idBrokerPSP = authEvent.data.brokerName
        paymentMethod = authEvent.data.paymentTypeCode
        this.totalAmount = totalAmountEuro
        transactionDetails =
          TransactionDetailsDto().apply {
            transaction =
              TransactionDto().apply {
                transactionStatus =
                  TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
                this.transactionId = transactionId
                this.fee = feeEuroCents
                this.amount = amountEuroCents
                grandTotal = totalAmountEuroCents
                this.errorCode = errorCode
                rrn = authCompletedEvent.data.rrn
                creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                psp =
                  PspDto().apply {
                    idPsp = authEvent.data.pspId
                    brokerName = authEvent.data.brokerName
                    idChannel = authEvent.data.pspChannelCode
                    businessName = authEvent.data.pspBusinessName
                    pspOnUs = authEvent.data.isPspOnUs
                  }
                authorizationCode = authCompletedEvent.data.authorizationCode
                this.timestampOperation = authCompletedEvent.data.timestampOperation
                paymentGateway = authEvent.data.paymentGateway.name
              }
            user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
            info =
              InfoDto().apply {
                type = authEvent.data.paymentTypeCode
                clientId = Transaction.ClientId.CHECKOUT.name
                brand =
                  (authEvent.data.transactionGatewayAuthorizationRequestedData
                      as PgsTransactionGatewayAuthorizationRequestedData)
                    .brand!!
                    .name
                brandLogo =
                  (authEvent.data.transactionGatewayAuthorizationRequestedData
                      as PgsTransactionGatewayAuthorizationRequestedData)
                    .logo
                    .toString()
                paymentMethodName = authEvent.data.paymentMethodName
              }
          }
        additionalPaymentInformations =
          CardAdditionalPaymentInformationsDto().apply {
            authorizationCode = authCompletedEvent.data.authorizationCode
            this.fee = feeEuro.toString()
            outcomePaymentGateway = expectedOutcome
            rrn = authCompletedEvent.data.rrn
            this.timestampOperation = expectedLocalDate
            this.totalAmount = totalAmountEuro.toString()
          }
      }

    assertEquals(expected, closePaymentRequestCaptor.value)
  }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for NPG payment gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.OK

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
            is RedirectTransactionGatewayAuthorizationData ->
              if (it.outcome == RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
                OutcomePaymentGatewayEnum.OK
              } else {
                OutcomePaymentGatewayEnum.KO
              }
          }
        }

      // Check close payment request information
      val expected =
        CardClosePaymentRequestV2Dto().apply {
          outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.OK
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          timestampOperation = OffsetDateTime.parse(authCompletedEvent.data.timestampOperation)
          this.fee = feeEuro
          idPSP = authEvent.data.pspId
          idChannel = authEvent.data.pspChannelCode
          idBrokerPSP = authEvent.data.brokerName
          paymentMethod = authEvent.data.paymentTypeCode
          this.totalAmount = totalAmountEuro
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = authCompletedEvent.data.authorizationCode
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .brand
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations =
            CardAdditionalPaymentInformationsDto().apply {
              authorizationCode = authCompletedEvent.data.authorizationCode
              this.fee = feeEuro.toString()
              outcomePaymentGateway = expectedOutcome
              rrn = authCompletedEvent.data.rrn
              timestampOperation = expectedTimestamp
              this.totalAmount = totalAmountEuro.toString()
            }
        }

      assertEquals(expected, closePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK authorization KO has additional properties and transaction details valued correctly for PGS payment gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.OK

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
            is RedirectTransactionGatewayAuthorizationData ->
              if (it.outcome == RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
                OutcomePaymentGatewayEnum.OK
              } else {
                OutcomePaymentGatewayEnum.KO
              }
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
      val transactionOutcome = ClosePaymentOutcome.KO

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
      val expected =
        CardClosePaymentRequestV2Dto().apply {
          outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .brand
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, closePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO has not additional properties and transaction details valued correctly for PGS gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO

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
      val expected =
        CardClosePaymentRequestV2Dto().apply {
          outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as PgsTransactionGatewayAuthorizationRequestedData)
                      .brand!!
                      .name
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as PgsTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, closePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO has not additional properties and transaction details valued correctly for NPG gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO

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

      val expected =
        CardClosePaymentRequestV2Dto().apply {
          outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .brand!!
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      assertEquals(expected, closePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK for cancelled transaction is correct for NPG gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO

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

      val expected =
        CardClosePaymentRequestV2Dto().apply {
          outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .brand!!
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      assertEquals(expected, closePaymentRequestCaptor.value)
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
            OperationResultDto.EXECUTED, "operationId", "paymentEndTOEndId", null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent()
      val transactionId = activatedEvent.transactionId
      val events =
        listOf(activatedEvent, authEvent, authCompletedEvent, closureRequestedEvent, closureError)
          as List<TransactionEvent<Any>>

      val npgOutcome = ClosePaymentOutcome.KO

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
        closePaymentResponse, nodeService.closePayment(TransactionId(transactionId), npgOutcome))
    }

  @Test
  fun `closePayment returns error for close payment missing authorization completed event`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authEvent = transactionAuthorizationRequestedEvent()

      val transactionId = activatedEvent.transactionId
      val events = listOf(activatedEvent, authEvent) as List<TransactionEvent<Any>>
      val transactionOutcome = ClosePaymentOutcome.OK

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

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for REDIRECT payment gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.OK
      val redirectTransactionGatewayAuthorizationRequestedData =
        redirectTransactionGatewayAuthorizationRequestedData()
          as RedirectTransactionGatewayAuthorizationRequestedData
      val redirectTransactionGatewayAuthorizationData =
        redirectTransactionGatewayAuthorizationData(
          RedirectTransactionGatewayAuthorizationData.Outcome.OK, "")
          as RedirectTransactionGatewayAuthorizationData

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
          redirectTransactionGatewayAuthorizationRequestedData)
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(redirectTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(redirectClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      val expectedTimestamp =
        OffsetDateTime.parse(
            authCompletedEvent.data.timestampOperation, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .truncatedTo(ChronoUnit.SECONDS)
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

      val expected =
        RedirectClosePaymentRequestV2Dto().apply {
          outcome = RedirectClosePaymentRequestV2Dto.OutcomeEnum.OK
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          timestampOperation = OffsetDateTime.parse(authCompletedEvent.data.timestampOperation)
          this.fee = feeEuro
          idPSP = authEvent.data.pspId
          idChannel = authEvent.data.pspChannelCode
          idBrokerPSP = authEvent.data.brokerName
          paymentMethod = authEvent.data.paymentTypeCode
          this.totalAmount = totalAmountEuro
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = authCompletedEvent.data.authorizationCode
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as RedirectTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations =
            RedirectAdditionalPaymentInformationsDto().apply {
              timestampOperation = OffsetDateTime.parse(expectedTimestamp)
              idPSPTransaction = authEvent.data.authorizationRequestId
              this.fee = feeEuro.toString()
              this.totalAmount = totalAmountEuro.toString()
              authorizationCode = authCompletedEvent.data.authorizationCode
              idTransaction = TRANSACTION_ID
            }
        }

      assertEquals(expected, redirectClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for cancelled transaction is correct for REDIRECT payment gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val redirectTransactionGatewayAuthorizationRequestedData =
        redirectTransactionGatewayAuthorizationRequestedData()
          as RedirectTransactionGatewayAuthorizationRequestedData
      val redirectTransactionGatewayAuthorizationData =
        redirectTransactionGatewayAuthorizationData(
          RedirectTransactionGatewayAuthorizationData.Outcome.KO, "errorCode")
          as RedirectTransactionGatewayAuthorizationData

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
          redirectTransactionGatewayAuthorizationRequestedData)
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(redirectTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(redirectClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        RedirectClosePaymentRequestV2Dto().apply {
          outcome = RedirectClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as RedirectTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as RedirectTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, redirectClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO is correct for REDIRECT payment gateway`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val redirectTransactionGatewayAuthorizationRequestedData =
        redirectTransactionGatewayAuthorizationRequestedData()
          as RedirectTransactionGatewayAuthorizationRequestedData
      val redirectTransactionGatewayAuthorizationData =
        redirectTransactionGatewayAuthorizationData(
          RedirectTransactionGatewayAuthorizationData.Outcome.KO, "errorCode")
          as RedirectTransactionGatewayAuthorizationData

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
          redirectTransactionGatewayAuthorizationRequestedData)
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(redirectTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(redirectClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        RedirectClosePaymentRequestV2Dto().apply {
          outcome = RedirectClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as RedirectTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as RedirectTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, redirectClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for PayPal method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.OK
      val paypalTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "PPAL", "npgSessionId", "npgConfirmPaymentSessionId")
      val paypalTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "PPAL",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            paypalTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(paypalTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(paypalClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      val expectedTimestamp =
        OffsetDateTime.parse(
            authCompletedEvent.data.timestampOperation, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .truncatedTo(ChronoUnit.SECONDS)
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

      val expected =
        PayPalClosePaymentRequestV2Dto().apply {
          outcome = PayPalClosePaymentRequestV2Dto.OutcomeEnum.OK
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          timestampOperation = OffsetDateTime.parse(authCompletedEvent.data.timestampOperation)
          this.fee = feeEuro
          idPSP = authEvent.data.pspId
          idChannel = authEvent.data.pspChannelCode
          idBrokerPSP = authEvent.data.brokerName
          paymentMethod = authEvent.data.paymentTypeCode
          this.totalAmount = totalAmountEuro
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = authCompletedEvent.data.authorizationCode
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations =
            PayPalAdditionalPaymentInformationsDto().apply {
              this.transactionId =
                (authCompletedEvent.data.transactionGatewayAuthorizationData
                    as NpgTransactionGatewayAuthorizationData)
                  .operationId
              this.pspTransactionId =
                (authCompletedEvent.data.transactionGatewayAuthorizationData
                    as NpgTransactionGatewayAuthorizationData)
                  .paymentEndToEndId
              timestampOperation = OffsetDateTime.parse(expectedTimestamp)
              this.fee = feeEuro.toString()
              this.totalAmount = totalAmountEuro.toString()
            }
        }

      assertEquals(expected, paypalClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for cancelled transaction is correct for PayPal method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val paypalTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "PPAL", "npgSessionId", "npgConfirmPaymentSessionId")
      val paypalTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "PPAL",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            paypalTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(paypalTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(paypalClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        PayPalClosePaymentRequestV2Dto().apply {
          outcome = PayPalClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as NpgTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, paypalClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO is correct for PayPal method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val paypalTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "PPAL", "npgSessionId", "npgConfirmPaymentSessionId")
      val paypalTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "PPAL",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            paypalTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(paypalTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(paypalClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        PayPalClosePaymentRequestV2Dto().apply {
          outcome = PayPalClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as NpgTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, paypalClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for BancomatPay method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.OK
      val bancomatPayTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "BPAY", "npgSessionId", "npgConfirmPaymentSessionId")
      val bancomatPayTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "BPAY",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            bancomatPayTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(bancomatPayTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(bancomatPayClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      val expectedTimestamp =
        OffsetDateTime.parse(
            authCompletedEvent.data.timestampOperation, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .truncatedTo(ChronoUnit.SECONDS)
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

      val expected =
        BancomatPayClosePaymentRequestV2Dto().apply {
          outcome = BancomatPayClosePaymentRequestV2Dto.OutcomeEnum.OK
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          timestampOperation = OffsetDateTime.parse(authCompletedEvent.data.timestampOperation)
          this.fee = feeEuro
          idPSP = authEvent.data.pspId
          idChannel = authEvent.data.pspChannelCode
          idBrokerPSP = authEvent.data.brokerName
          paymentMethod = authEvent.data.paymentTypeCode
          this.totalAmount = totalAmountEuro
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = authCompletedEvent.data.authorizationCode
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations =
            BancomatPayAdditionalPaymentInformationsDto().apply {
              this.transactionId =
                (authCompletedEvent.data.transactionGatewayAuthorizationData
                    as NpgTransactionGatewayAuthorizationData)
                  .operationId
              this.outcomePaymentGateway =
                BancomatPayAdditionalPaymentInformationsDto.OutcomePaymentGatewayEnum.OK
              this.totalAmount = totalAmountEuro.toString()
              this.fee = feeEuro.toString()
              this.timestampOperation = OffsetDateTime.parse(expectedTimestamp)
              this.authorizationCode =
                (authCompletedEvent.data.transactionGatewayAuthorizationData
                    as NpgTransactionGatewayAuthorizationData)
                  .operationId
              this.seteMail(null)
            }
        }

      assertEquals(expected, bancomatPayClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for cancelled transaction is correct for BancomatPay method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val bancomatPayTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "BPAY", "npgSessionId", "npgConfirmPaymentSessionId")
      val bancomatPayTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "BPAY",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            bancomatPayTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(bancomatPayTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(bancomatPayClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        BancomatPayClosePaymentRequestV2Dto().apply {
          outcome = BancomatPayClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as NpgTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, bancomatPayClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO is correct for BancomatPay method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val bancomatPayTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "BPAY", "npgSessionId", "npgConfirmPaymentSessionId")
      val bancomatPayTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "BPAY",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            bancomatPayTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(bancomatPayTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(bancomatPayClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        BancomatPayClosePaymentRequestV2Dto().apply {
          outcome = BancomatPayClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as NpgTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, bancomatPayClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment OK has additional properties and transaction details valued correctly for MyBank method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.OK
      val myBankTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "MYBK", "npgSessionId", "npgConfirmPaymentSessionId")
      val myBankTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "MYBK",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            myBankTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(myBankTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(bancomatPayClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuro = EuroUtils.euroCentsToEuro(fee)
      val totalAmountEuro = EuroUtils.euroCentsToEuro(totalAmount)

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))
      val expectedTimestamp =
        OffsetDateTime.parse(
            authCompletedEvent.data.timestampOperation, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .truncatedTo(ChronoUnit.SECONDS)
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

      val expected =
        MyBankClosePaymentRequestV2Dto().apply {
          outcome = MyBankClosePaymentRequestV2Dto.OutcomeEnum.OK
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          timestampOperation = OffsetDateTime.parse(authCompletedEvent.data.timestampOperation)
          this.fee = feeEuro
          idPSP = authEvent.data.pspId
          idChannel = authEvent.data.pspChannelCode
          idBrokerPSP = authEvent.data.brokerName
          paymentMethod = authEvent.data.paymentTypeCode
          this.totalAmount = totalAmountEuro
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_CONFIRMED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode = errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = authCompletedEvent.data.authorizationCode
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations =
            MyBankAdditionalPaymentInformationsDto().apply {
              this.transactionId =
                (authCompletedEvent.data.transactionGatewayAuthorizationData
                    as NpgTransactionGatewayAuthorizationData)
                  .operationId
              this.mybankTransactionId =
                (authCompletedEvent.data.transactionGatewayAuthorizationData
                    as NpgTransactionGatewayAuthorizationData)
                  .paymentEndToEndId
              this.totalAmount = totalAmountEuro.toString()
              this.fee = feeEuro.toString()
              this.validationServiceId = ""
              this.timestampOperation = OffsetDateTime.parse(expectedTimestamp)
              this.seteMail(null)
            }
        }

      assertEquals(expected, bancomatPayClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for cancelled transaction is correct for MyBank method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val myBankTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "MYBK", "npgSessionId", "npgConfirmPaymentSessionId")
      val myBankTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "MYBK",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            myBankTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(myBankTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(bancomatPayClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        MyBankClosePaymentRequestV2Dto().apply {
          outcome = MyBankClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as NpgTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, bancomatPayClosePaymentRequestCaptor.value)
    }

  @Test
  fun `ClosePaymentRequestV2Dto for close payment KO for authorization KO is correct for MyBank method`() =
    runTest {
      val transactionOutcome = ClosePaymentOutcome.KO
      val myBankTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI, "MYBK", "npgSessionId", "npgConfirmPaymentSessionId")
      val myBankTransactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED)

      val activatedEvent = transactionActivateEvent()
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            "MYBK",
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            myBankTransactionGatewayAuthorizationRequestedData))
      val authCompletedEvent =
        transactionAuthorizationCompletedEvent(myBankTransactionGatewayAuthorizationData)
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

      given(nodeClient.closePayment(capture(bancomatPayClosePaymentRequestCaptor)))
        .willReturn(Mono.just(closePaymentResponse))

      val fee = authEvent.data.fee
      val amount = authEvent.data.amount
      val totalAmount = amount + fee

      val feeEuroCents = BigDecimal(fee)
      val amountEuroCents = BigDecimal(amount)
      val totalAmountEuroCents = BigDecimal(totalAmount)

      /* test */
      assertEquals(
        closePaymentResponse,
        nodeService.closePayment(TransactionId(transactionId), transactionOutcome))

      val expected =
        MyBankClosePaymentRequestV2Dto().apply {
          outcome = MyBankClosePaymentRequestV2Dto.OutcomeEnum.KO
          this.transactionId = transactionId
          paymentTokens =
            activatedEvent.data.paymentNotices.map { paymentNotice -> paymentNotice.paymentToken }
          transactionDetails =
            TransactionDetailsDto().apply {
              transaction =
                TransactionDto().apply {
                  transactionStatus =
                    TransactionDetailsStatusEnum.TRANSACTION_DETAILS_STATUS_DENIED.status
                  this.transactionId = transactionId
                  this.fee = feeEuroCents
                  this.amount = amountEuroCents
                  grandTotal = totalAmountEuroCents
                  this.errorCode =
                    (authCompletedEvent.data.transactionGatewayAuthorizationData
                        as NpgTransactionGatewayAuthorizationData)
                      .errorCode
                  rrn = authCompletedEvent.data.rrn
                  creationDate = ZonedDateTime.parse(activatedEvent.creationDate).toOffsetDateTime()
                  psp =
                    PspDto().apply {
                      idPsp = authEvent.data.pspId
                      brokerName = authEvent.data.brokerName
                      idChannel = authEvent.data.pspChannelCode
                      businessName = authEvent.data.pspBusinessName
                      pspOnUs = authEvent.data.isPspOnUs
                    }
                  authorizationCode = null
                  timestampOperation = authCompletedEvent.data.timestampOperation
                  paymentGateway = authEvent.data.paymentGateway.name
                }
              user = UserDto().apply { type = UserDto.TypeEnum.GUEST }
              info =
                InfoDto().apply {
                  type = authEvent.data.paymentTypeCode
                  clientId = Transaction.ClientId.CHECKOUT.name
                  brand = authEvent.data.paymentTypeCode
                  brandLogo =
                    (authEvent.data.transactionGatewayAuthorizationRequestedData
                        as NpgTransactionGatewayAuthorizationRequestedData)
                      .logo
                      .toString()
                  paymentMethodName = authEvent.data.paymentMethodName
                }
            }
          additionalPaymentInformations = null
        }

      assertEquals(expected, bancomatPayClosePaymentRequestCaptor.value)
    }
}
