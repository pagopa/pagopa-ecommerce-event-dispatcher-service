package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v1.*
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionsRefundEventsConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val refundRetryService: RefundRetryService = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  @Captor
  private lateinit var refundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val transactionRefundedEventsConsumer =
    TransactionsRefundQueueConsumer(
      paymentGatewayClient = paymentGatewayClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      refundRetryService = refundRetryService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils)

  companion object {
    @JvmStatic
    private fun vposStatusesToEnqueueRetryEventMapping() =
      Stream.of(
        Arguments.of(
          VposDeleteResponseDto.StatusEnum.AUTHORIZED,
          true,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT),
        Arguments.of(
          VposDeleteResponseDto.StatusEnum.CREATED,
          true,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT),
        Arguments.of(
          VposDeleteResponseDto.StatusEnum.CANCELLED,
          false,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT),
        Arguments.of(
          VposDeleteResponseDto.StatusEnum.DENIED,
          false,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT),
      )

    @JvmStatic
    private fun xpayStatusesToEnqueueRetryEventMapping() =
      Stream.of(
        Arguments.of(
          XPayRefundResponse200Dto.StatusEnum.AUTHORIZED,
          true,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT),
        Arguments.of(
          XPayRefundResponse200Dto.StatusEnum.CREATED,
          true,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT),
        Arguments.of(
          XPayRefundResponse200Dto.StatusEnum.CANCELLED,
          false,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT),
        Arguments.of(
          XPayRefundResponse200Dto.StatusEnum.DENIED,
          false,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT),
      )
  }

  @Test
  fun `consumer processes refund request event correctly with pgs refund`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse =
      VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.CANCELLED }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestVPosRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @Test
  fun `consumer processes refund request event correctly with pgs refund with legacy event`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val gatewayClientResponse =
        VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.CANCELLED }

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closedEvent,
          refundRequestedEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

      /* test */
      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
              refundRequestedEvent as TransactionRefundRequestedEvent) to null,
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(paymentGatewayClient, Mockito.times(1))
        .requestVPosRefund(
          UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
        TransactionEventCode.valueOf(storedEvent.eventCode))
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    }

  @Test
  fun `consumer processes refund request event correctly with pgs refund (xpay)`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse =
      XPayRefundResponse200Dto().apply { status = XPayRefundResponse200Dto.StatusEnum.CANCELLED }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestXPayRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @Test
  fun `consumer processes refund request event for a transaction without refund requested`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
        )
      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }

      /* test */

      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
              refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(paymentGatewayClient, Mockito.times(0))
        .requestVPosRefund(
          UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    }

  @Test
  fun `consumer enqueue refund retry event for KO response from PGS (vpos)`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse =
      VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.AUTHORIZED }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

    /* test */

    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestVPosRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @Test
  fun `consumer enqueue refund retry event for KO response from PGS (vpos) with legacy event`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val gatewayClientResponse =
        VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.AUTHORIZED }

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closedEvent,
          refundRequestedEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(refundRetryService.enqueueRetryEvent(any(), any(), isNull())).willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

      /* test */

      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
              refundRequestedEvent as TransactionRefundRequestedEvent) to null,
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(paymentGatewayClient, Mockito.times(1))
        .requestVPosRefund(
          UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), isNull())

      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
        TransactionEventCode.valueOf(storedEvent.eventCode))
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    }

  @Test
  fun `consumer enqueue refund retry event for KO response from PGS (xpay)`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse =
      XPayRefundResponse200Dto().apply { status = XPayRefundResponse200Dto.StatusEnum.AUTHORIZED }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

    /* test */

    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestXPayRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @ParameterizedTest
  @MethodSource("vposStatusesToEnqueueRetryEventMapping")
  fun `consumer should handle response from PGS status correctly (vpos)`(
    pgsStatus: VposDeleteResponseDto.StatusEnum,
    shouldWriteErrorEvent: Boolean,
    expectedWrittenEventStatus: TransactionEventCode
  ) = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse = VposDeleteResponseDto().apply { status = pgsStatus }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

    /* test */
    Hooks.onOperatorDebug()

    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestVPosRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(
        refundRetryService,
        times(
          if (shouldWriteErrorEvent) {
            1
          } else {
            0
          }))
      .enqueueRetryEvent(any(), any(), any())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(expectedWrittenEventStatus, TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @ParameterizedTest
  @MethodSource("xpayStatusesToEnqueueRetryEventMapping")
  fun `consumer should handle response from PGS status correctly (xpay)`(
    pgsStatus: XPayRefundResponse200Dto.StatusEnum,
    shouldWriteErrorEvent: Boolean,
    expectedWrittenEventStatus: TransactionEventCode
  ) = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse = XPayRefundResponse200Dto().apply { status = pgsStatus }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

    /* test */

    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestXPayRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(
        refundRetryService,
        times(
          if (shouldWriteErrorEvent) {
            1
          } else {
            0
          }))
      .enqueueRetryEvent(any(), any(), any())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(expectedWrittenEventStatus, TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @Test
  fun `consumer does not enqueue refund retry with error 409 response from PGS (xpay)`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willThrow(RefundNotAllowedException(UUID.randomUUID()))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

    /* test */

    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
            refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestXPayRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @Test
  fun `consumer does not enqueue refund retry event for error 409 response from PGS (vpos)`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closedEvent,
          refundRequestedEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willThrow(RefundNotAllowedException(UUID.randomUUID()))
      given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

      /* test */

      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(
              refundRequestedEvent as TransactionRefundRequestedEvent) to MOCK_TRACING_INFO,
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(paymentGatewayClient, Mockito.times(1))
        .requestVPosRefund(
          UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())

      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
        TransactionEventCode.valueOf(storedEvent.eventCode))
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    }
}
