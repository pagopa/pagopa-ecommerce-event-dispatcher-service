package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionTracing
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionsViewProjectionHandler
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundOutcomeDto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto as RedirectRefundResponseDto
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
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
import org.springframework.core.env.Environment
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionsRefundEventsConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()
  private val refundDelayFromAuthRequestMinutes = 10L
  private val eventProcessingDelaySeconds = 10L

  private val npgService: NpgService =
    NpgService(
      authorizationStateRetrieverService,
      refundDelayFromAuthRequestMinutes,
      eventProcessingDelaySeconds)

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val refundService: RefundService = mock()

  private val refundRetryService: RefundRetryService = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData> =
    mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  private lateinit var mockOpenTelemetryUtils: OpenTelemetryUtils

  @Captor
  private lateinit var refundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRefundedData>>

  @Captor private lateinit var queueArgumentCaptor: ArgumentCaptor<BinaryData>

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val transactionTracing = getTransactionTracingMock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()
  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  var mockedEnv: Environment = mock<Environment>() as Environment

  val ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG = "transactionsview.update.enabled"

  @BeforeEach
  fun setUp() {
    TransactionsViewProjectionHandler.env = mockedEnv
  }

  private val transactionRefundedEventsConsumer =
    TransactionsRefundQueueConsumer(
      paymentGatewayClient = paymentGatewayClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      refundService = refundService,
      refundRetryService = refundRetryService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2,
      npgService = npgService,
      transactionTracing = transactionTracing,
    )

  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()

  companion object {
    @JvmStatic
    private fun redirectClientsMappingMethodSource(): Stream<Arguments> =
      Stream.of(
        Arguments.of(ClientId.CHECKOUT, "CHECKOUT"),
        Arguments.of(ClientId.IO, "IO"),
        Arguments.of(ClientId.CHECKOUT_CART, "CHECKOUT"))
  }

  @Test
  fun `consumer processes refund request event for a transaction without refund requested`() {
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED,
            "operationId",
            "paymentEndToEndId",
            "errorCode",
            "validationServiceId"))
          as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID,
          TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
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
            Either.right(
              QueueEvent(
                refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(refundService, Mockito.times(0))
        .requestNpgRefund(any(), any(), any(), any(), any(), any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(refundRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `consumer doesn't process refund request event correctly with unknown payment gateway`() {
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val paymentMethodName = "CARDS"
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentMethodName =
        paymentMethodName
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway = null

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID,
          TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closedEvent,
          refundRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

      /* test */
      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right(
              QueueEvent(
                refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectError(RuntimeException::class.java)
        .verify()

      /* Asserts */
      val expectedOperationId = NPG_OPERATION_ID
      val expectedIdempotencyKey = TransactionId(TRANSACTION_ID).uuid
      val correlationId = UUID.randomUUID().toString()
      val expectedAmount =
        BigDecimal.valueOf(
          (activationEvent as TransactionActivatedEvent)
            .data
            .paymentNotices
            .sumOf { it.amount }
            .toLong() +
            (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.fee)
      val expectedPspId =
        (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.pspId
      verify(checkpointer, Mockito.times(1)).success()
      verify(refundService, Mockito.times(0))
        .requestNpgRefund(
          operationId = expectedOperationId,
          idempotenceKey = expectedIdempotencyKey,
          amount = expectedAmount,
          pspId = expectedPspId,
          correlationId = correlationId,
          paymentMethod =
            NpgClient.PaymentMethod.valueOf(authorizationRequestEvent.data.paymentMethodName))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT.toString(), storedEvent.eventCode)
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `consumer processes refund request event correctly with npg refund`() {
    runTest {
      val correlationId = UUID.randomUUID().toString()
      val activationEvent =
        transactionActivateEvent(NpgTransactionGatewayActivationData("orderId", correlationId))
          as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID,
          TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val refundServiceNpgResponse =
        RefundResponseDto().apply {
          operationId = "operationId"
          operationTime = "operationTime"
        }

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
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
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.just(refundServiceNpgResponse))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

      /* test */
      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right(
              QueueEvent(
                refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      val expectedOperationId = NPG_OPERATION_ID
      val expectedIdempotencyKey = TransactionId(TRANSACTION_ID).uuid
      val expectedAmount =
        BigDecimal.valueOf(
          (activationEvent as TransactionActivatedEvent)
            .data
            .paymentNotices
            .sumOf { it.amount }
            .toLong() +
            (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.fee)
      val expectedPspId =
        (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.pspId
      verify(checkpointer, Mockito.times(1)).success()
      verify(refundService, Mockito.times(1))
        .requestNpgRefund(
          operationId = expectedOperationId,
          idempotenceKey = expectedIdempotencyKey,
          amount = expectedAmount,
          pspId = expectedPspId,
          correlationId = correlationId,
          paymentMethod =
            NpgClient.PaymentMethod.valueOf(authorizationRequestEvent.data.paymentMethodName))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT.toString(), storedEvent.eventCode)
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
      verify(transactionTracing, times(1))
        .addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `consumer does not enqueue refund retry event for RefundNotAllowedException response from NPG`() {
    runTest {
      val correlationId = UUID.randomUUID().toString()
      val activationEvent =
        transactionActivateEvent(NpgTransactionGatewayActivationData("orderId", correlationId))
          as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
          as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentMethodName =
        NpgClient.PaymentMethod.CARDS.serviceName
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID,
          TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
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
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willThrow(RefundNotAllowedException(transaction.transactionId))
      given(refundRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(queueArgumentCaptor), any()))
        .willReturn(mono {})
      /* test */

      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right(
              QueueEvent(
                refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      val expectedOperationId = NPG_OPERATION_ID
      val expectedIdempotencyKey = TransactionId(TRANSACTION_ID).uuid
      val expectedAmount =
        BigDecimal.valueOf(
          (activationEvent as TransactionActivatedEvent)
            .data
            .paymentNotices
            .sumOf { it.amount }
            .toLong() +
            (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.fee)
      val expectedPspId =
        (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.pspId
      verify(checkpointer, Mockito.times(1)).success()
      verify(refundService, Mockito.times(1))
        .requestNpgRefund(
          operationId = expectedOperationId,
          idempotenceKey = expectedIdempotencyKey,
          amount = expectedAmount,
          pspId = expectedPspId,
          correlationId = correlationId,
          paymentMethod =
            NpgClient.PaymentMethod.valueOf(authorizationRequestEvent.data.paymentMethodName))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          any<BinaryData>(),
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
      assertEquals(
        String(
          jsonSerializerV2.serializeToBytes(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
          StandardCharsets.UTF_8),
        String(queueArgumentCaptor.value.toBytes(), StandardCharsets.UTF_8))

      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT.toString(), storedEvent.eventCode)
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithRefundRequested`() {
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG

      val transactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID,
          TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closedEvent,
          refundRequestedEvent)

      val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionWithRefundRequested

      assertEquals(
        getAuthorizationCompletedData(transaction, npgService).block(),
        transactionGatewayAuthorizationData)
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithCompletedAuthorization`() {
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG

      val transactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
          as TransactionEvent<Any>

      val events = listOf(activationEvent, authorizationRequestEvent, authorizationCompleteEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithCompletedAuthorization

      assertEquals(
        getAuthorizationCompletedData(transaction, npgService).block(),
        transactionGatewayAuthorizationData)
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithClosureError`() {
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG

      val transactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureError = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureError)

      val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionWithClosureError

      assertEquals(
        getAuthorizationCompletedData(transaction, npgService).block(),
        transactionGatewayAuthorizationData)
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionExpired`() {
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG

      val transactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
          as TransactionEvent<Any>

      val events: MutableList<TransactionEvent<Any>> =
        mutableListOf(activationEvent, authorizationRequestEvent, authorizationCompleteEvent)

      val expiredEvent =
        transactionExpiredEvent(reduceEvents(*events.toTypedArray())) as TransactionEvent<Any>

      events.add(expiredEvent)

      val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionExpired

      assertEquals(
        getAuthorizationCompletedData(transaction, npgService).block(),
        transactionGatewayAuthorizationData)
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithRequestedAuthorization`() {
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT

      val events = listOf(activationEvent, authorizationRequestEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      assertNull(getAuthorizationCompletedData(transaction, npgService).block())
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }
  }

  @Test
  fun `consumer does not call refund if authorization was not requested`() {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.ACTIVATED))
        as TransactionEvent<Any>

    val events = listOf(activationEvent, refundRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verifyNoInteractions(refundService)
    verify(refundService, times(0)).requestNpgRefund(any(), any(), any(), any(), any(), any())
    verify(refundService, times(0)).requestRedirectRefund(any(), any(), any(), any(), any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(refundRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @MethodSource("redirectClientsMappingMethodSource")
  fun `consumer processes refund request event correctly with for redirect transaction`(
    touchPoint: ClientId,
    expectedMappedTouchPoint: String
  ) = runTest {
    val activationEvent =
      transactionActivateEvent().apply { this.data.clientId = touchPoint } as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
        as TransactionEvent<Any>

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        redirectTransactionGatewayAuthorizationData(
          RedirectTransactionGatewayAuthorizationData.Outcome.OK, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val refundRedirectResponse =
      RedirectRefundResponseDto().apply {
        idTransaction = TRANSACTION_ID
        outcome = RefundOutcomeDto.OK
      }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closedEvent,
        refundRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(refundService.requestRedirectRefund(any(), any(), any(), any(), any()))
      .willReturn(Mono.just(refundRedirectResponse))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    val expectedTransactionId = TRANSACTION_ID
    val expectedPspTransactionId = AUTHORIZATION_REQUEST_ID
    val expectedPaymentTypeCode =
      (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.paymentTypeCode
    val expectedPspId =
      (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.pspId
    verify(checkpointer, Mockito.times(1)).success()
    verify(refundService, Mockito.times(1))
      .requestRedirectRefund(
        transactionId = TransactionId(expectedTransactionId),
        touchpoint = expectedMappedTouchPoint,
        pspTransactionId = expectedPspTransactionId,
        paymentTypeCode = expectedPaymentTypeCode,
        pspId = expectedPspId)
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_REFUNDED_EVENT.toString(), storedEvent.eventCode)
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    verify(transactionTracing, times(1)).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, times(1))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer return error processing refund for unhandled redirect client id`() = runTest {
    val activationEvent =
      transactionActivateEvent().apply { this.data.clientId = null } as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
        as TransactionEvent<Any>

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        redirectTransactionGatewayAuthorizationData(
          RedirectTransactionGatewayAuthorizationData.Outcome.OK, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closedEvent,
        refundRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })
    given(refundRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    Hooks.onOperatorDebug()
    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */

    verify(checkpointer, Mockito.times(1)).success()
    verify(refundService, Mockito.times(0))
      .requestRedirectRefund(
        transactionId = any(),
        touchpoint = any(),
        pspTransactionId = any(),
        paymentTypeCode = any(),
        pspId = any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    verify(transactionTracing, times(1)).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  ////

  @Test
  fun `consumer processes refund request event correctly with npg refund ue`() = runTest {
    val correlationId = UUID.randomUUID().toString()
    val activationEvent =
      transactionActivateEvent(NpgTransactionGatewayActivationData("orderId", correlationId))
        as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.NPG

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val refundServiceNpgResponse =
      RefundResponseDto().apply {
        operationId = "operationId"
        operationTime = "operationTime"
      }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
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
    given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
      .willReturn(Mono.just(refundServiceNpgResponse))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(transactionTracing, times(1)).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    val attributesCaptor = ArgumentCaptor.forClass(Attributes::class.java)
    verify(mockOpenTelemetryUtils, times(1))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), capture(attributesCaptor))
    val capturedAttributes = attributesCaptor.value

    assertEquals(
      Transaction.ClientId.CHECKOUT.toString(),
      capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.CLIENTID)))
    assertEquals(
      NpgClient.PaymentMethod.CARDS.toString(),
      capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.PAYMENTMETHOD)))
    assertEquals("pspId", capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)))
    /*assertEquals(
    0,
    capturedAttributes.get(AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME)))*/
    assertEquals(
      TransactionStatusDto.REFUNDED.toString(),
      capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)))
    assertEquals(
      TransactionTestUtils.TRANSACTION_ID,
      capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)))
  }

  @Test
  fun `consumer handles redirect refund KO response outcome writing to DLQ`() = runTest {
    val activationEvent =
      transactionActivateEvent().apply { this.data.clientId = ClientId.CHECKOUT }
        as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
        as TransactionEvent<Any>

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        redirectTransactionGatewayAuthorizationData(
          RedirectTransactionGatewayAuthorizationData.Outcome.OK, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val refundRedirectResponse =
      RedirectRefundResponseDto().apply {
        idTransaction = TRANSACTION_ID
        outcome = RefundOutcomeDto.KO
      }

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closedEvent,
        refundRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(refundService.requestRedirectRefund(any(), any(), any(), any(), any()))
      .willReturn(Mono.just(refundRedirectResponse))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
          capture(queueArgumentCaptor), any()))
      .willReturn(mono {})

    /* test */
    StepVerifier.create(
        transactionRefundedEventsConsumer.messageReceiver(
          Either.right(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    val expectedTransactionId = TRANSACTION_ID
    val expectedPspTransactionId = AUTHORIZATION_REQUEST_ID
    val expectedPaymentTypeCode =
      (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.paymentTypeCode
    val expectedPspId =
      (authorizationRequestEvent as TransactionAuthorizationRequestedEvent).data.pspId
    verify(checkpointer, Mockito.times(1)).success()
    verify(refundService, Mockito.times(1))
      .requestRedirectRefund(
        transactionId = TransactionId(expectedTransactionId),
        touchpoint = ClientId.CHECKOUT.toString(),
        pspTransactionId = expectedPspTransactionId,
        paymentTypeCode = expectedPaymentTypeCode,
        pspId = expectedPspId)
    verify(transactionsRefundedEventStoreRepository, Mockito.times(2)).save(any())
    verify(refundRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT.toString(), storedEvent.eventCode)
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    verify(transactionTracing, times(0)).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, times(0))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        any<BinaryData>(),
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode =
              TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    assertEquals(
      String(
        jsonSerializerV2.serializeToBytes(
          QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
        StandardCharsets.UTF_8),
      String(queueArgumentCaptor.value.toBytes(), StandardCharsets.UTF_8))
  }

  private fun getTransactionTracingMock(): TransactionTracing {
    // Create a mock of OpenTelemetryUtils
    val mockOpenTelemetryUtils: OpenTelemetryUtils = mock()

    // Create a real TransactionTracing instance with the mock OpenTelemetryUtils
    val transactionTracing = TransactionTracing(mockOpenTelemetryUtils)

    val transactionTracingSpy = spy(transactionTracing)

    // Store the mockOpenTelemetryUtils for later verification
    this.mockOpenTelemetryUtils = mockOpenTelemetryUtils

    return transactionTracingSpy
  }
}
