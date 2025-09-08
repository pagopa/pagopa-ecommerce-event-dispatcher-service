package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.client.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.ClosureErrorData.ErrorType
import it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationRequestedData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureError
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.redis.reactivetemplatewrappers.v2.ReactivePaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils.ClosePaymentNodoStatusUpdate
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils.UserCancelClosePaymentNodoStatusUpdate
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NodeService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.*
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
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
import org.junit.jupiter.params.provider.*
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.core.env.Environment
import org.springframework.http.HttpStatus
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(MockitoExtension::class)
class ClosePaymentHelperTests {
  private val checkpointer: Checkpointer = mock()

  private val nodeService: NodeService = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionClosureErrorEventStoreRepository:
    TransactionsEventStoreRepository<ClosureErrorData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val closureRetryService: ClosureRetryService = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData> =
    mock()

  private val transactionClosedEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData> =
    mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  private val reactivePaymentRequestInfoRedisTemplateWrapper:
    ReactivePaymentRequestInfoRedisTemplateWrapper =
    mock()

  private val refundQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionTracing = getTransactionTracingMock()

  private lateinit var mockOpenTelemetryUtils: OpenTelemetryUtils

  @Captor private lateinit var viewArgumentCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var refundedEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRefundedData>>

  @Captor
  private lateinit var closedEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<TransactionClosureData>>

  @Captor
  private lateinit var closureErrorEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<ClosureErrorData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>
  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()
  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()
  private val updateTransactionStatusTracerUtils: UpdateTransactionStatusTracerUtils = mock {}
  private val refundDelayFromAuthRequestMinutes = 10L
  private val eventProcessingDelaySeconds = 10L

  var mockedEnv: Environment = mock<Environment>() as Environment

  val ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG = "transactionsview.update.enabled"

  @BeforeEach
  fun setUp() {
    TransactionsViewProjectionHandler.env = mockedEnv
  }

  private val closePaymentHelper =
    ClosePaymentHelper(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionClosureSentEventRepository = transactionClosedEventRepository,
      transactionClosureErrorEventStoreRepository = transactionClosureErrorEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      nodeService = nodeService,
      closureRetryService = closureRetryService,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      reactivePaymentRequestInfoRedisTemplateWrapper =
        reactivePaymentRequestInfoRedisTemplateWrapper,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2,
      npgService =
        NpgService(
          authorizationStateRetrieverService,
          refundDelayFromAuthRequestMinutes,
          eventProcessingDelaySeconds),
      refundQueueAsyncClient = refundQueueAsyncClient,
      transientQueueTTLSeconds = TRANSIENT_QUEUE_TTL_SECONDS,
      updateTransactionStatusTracerUtils = updateTransactionStatusTracerUtils,
      transactionTracing = transactionTracing)

  @Test
  fun `consumer throw exception when the transaction authorization is performed via pgs`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.VPOS,
          PgsTransactionGatewayAuthorizationRequestedData())
          as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData("000", AuthorizationResultDto.OK))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */
      Hooks.onOperatorDebug()
      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectError()
        .verify()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0)).closePayment(any(), any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())

      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer throw exception when the transaction is authorized via pgs `() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData("000", AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    val expectedUpdatedTransaction =
      transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
        .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))

    doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
    /* test */
    Hooks.onOperatorDebug()
    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectError()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(0)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())

    verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(
                object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                jsonSerializerV2)
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode = TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
      )
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", "errorCode", "id"))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      expectedUpdatedTransaction.lastProcessedEventAt =
        ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
          .toInstant()
          .toEpochMilli()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      assertEquals(TransactionStatusDto.CLOSED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)

      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", "errorCode", "id"))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)

      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare closure error message correctly with KO closure outcome for unauthorized transaction`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.DECLINED, "operationId", "999", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))
      transactionDocument.closureErrorData =
        ClosureErrorData(
          HttpStatus.INTERNAL_SERVER_ERROR,
          "node error description",
          ErrorType.KO_RESPONSE_RECEIVED)
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      expectedUpdatedTransaction.lastProcessedEventAt =
        ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
          .toInstant()
          .toEpochMilli()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(TransactionStatusDto.UNAUTHORIZED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      assertNull(viewArgumentCaptor.value.closureErrorData)
      verify(transactionTracing, times(1))
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare closure error message correctly with KO closure outcome for unauthorized transaction with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.DECLINED, "operationId", "999", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))
      transactionDocument.closureErrorData =
        ClosureErrorData(
          HttpStatus.INTERNAL_SERVER_ERROR,
          "node error description",
          ErrorType.KO_RESPONSE_RECEIVED)
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, times(1))
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer error processing bare closure error message for authorized transaction with missing authorizationResultDto value`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(null) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      val expectedClosureEvent =
        TransactionClosureFailedEvent(
          activationEvent.transactionId,
          TransactionClosureData(TransactionClosureData.Outcome.OK, false))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO)
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(updateTransactionStatusTracerUtils, times(0)).traceStatusUpdateOperation(any())
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer error processing bare closure error message for closure error aggregate with unexpected transactionAtPreviousStep`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val emptyTransactionMock: EmptyTransaction = mock()
      val transactionWithClosureError: TransactionWithClosureError = mock()
      val fakeTransactionAtPreviousState = transactionActivated(ZonedDateTime.now().toString())

      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events = listOf(activatedEvent as TransactionEvent<Any>)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(emptyTransactionMock.applyEvent(any())).willReturn(transactionWithClosureError)
      given(transactionWithClosureError.transactionId).willReturn(TransactionId(TRANSACTION_ID))
      given(transactionWithClosureError.status).willReturn(TransactionStatusDto.CLOSURE_ERROR)
      given(transactionWithClosureError.transactionAtPreviousState)
        .willReturn(fakeTransactionAtPreviousState)
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            emptyTransactionMock))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO)
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(updateTransactionStatusTracerUtils, times(0)).traceStatusUpdateOperation(any())
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes closure retry message correctly`() = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val closureRetriedEvent = transactionClosureRetriedEvent(0)

    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationUpdateEvent,
        closureRequestedEvent,
        closureErrorEvent)

    val expectedUpdatedTransaction =
      transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
        .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))
    transactionDocument.closureErrorData =
      ClosureErrorData(
        HttpStatus.INTERNAL_SERVER_ERROR, "node error description", ErrorType.KO_RESPONSE_RECEIVED)
    val expectedClosureEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(any())).willReturn(Mono.just(expectedClosureEvent))
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.retried(QueueEvent(closureRetriedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    expectedUpdatedTransaction.lastProcessedEventAt =
      ZonedDateTime.parse(expectedClosureEvent.creationDate).toInstant().toEpochMilli()
    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.OK.toString(), Optional.empty())))
    assertNull(viewArgumentCaptor.allValues[0].closureErrorData)
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer processes closure retry message correctly with no transactions-view update if feature flag disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val closureRetriedEvent = transactionClosureRetriedEvent(0)

      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationUpdateEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationUpdateEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))
      transactionDocument.closureErrorData =
        ClosureErrorData(
          HttpStatus.INTERNAL_SERVER_ERROR,
          "node error description",
          ErrorType.KO_RESPONSE_RECEIVED)
      val expectedClosureEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.retried(QueueEvent(closureRetriedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer process doesn't modify db on invalid transaction status`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
        as TransactionEvent<Any>

    val events = listOf(activationEvent, authorizationRequestEvent, authorizationUpdateEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(0)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(
                object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                jsonSerializerV2)
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode = TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
      )
    verify(updateTransactionStatusTracerUtils, times(0)).traceStatusUpdateOperation(any())
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response outcome KO for transaction in closure error status`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(2)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(TransactionStatusDto.CLOSED, TransactionStatusDto.REFUND_REQUESTED)
      val expectedEventsCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        )
      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response outcome KO for transaction in closure error status with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(TransactionStatusDto.CLOSED, TransactionStatusDto.REFUND_REQUESTED)
      val expectedEventsCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        )
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer try to perform refund for authorized transaction with pgs and throws error`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData("000", AuthorizationResultDto.OK))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectError()
        .verify()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0)).closePayment(any(), any())
      verify(refundQueueAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(updateTransactionStatusTracerUtils, times(0)).traceStatusUpdateOperation(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer enqueue retry event in case of error processing the received event for transaction in closure error status`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture()))
        .willReturnConsecutively(
          listOf(
            Mono.error(RuntimeException("Error updating view")),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          closureRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(2)).save(any())

      verify(closureRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(TransactionStatusDto.CLOSED, TransactionStatusDto.CLOSURE_ERROR)

      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      assertEquals(0, retryCountCaptor.value)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))
      viewArgumentCaptor.allValues.last().let {
        assertNull(it.closureErrorData!!.errorDescription)
        assertNull(it.closureErrorData!!.httpErrorCode)
        assertEquals(ErrorType.COMMUNICATION_ERROR, it.closureErrorData!!.errorType)
      }
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer enqueue retry event in case of error processing the input retry event`() = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val retryCount = 1
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>
    val closureRetriedEvent = transactionClosureRetriedEvent(retryCount) as TransactionEvent<Any>
    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent,
        closureRetriedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionClosureErrorEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.save(viewArgumentCaptor.capture()))
      .willReturnConsecutively(
        listOf(
          Mono.error(RuntimeException("Error updating view")),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
    given(
        closureRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.retried(
            QueueEvent(closureRetriedEvent as TransactionClosureRetriedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(2)).save(any())
    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

    val expectedViewUpdateStatuses =
      listOf(TransactionStatusDto.CLOSED, TransactionStatusDto.CLOSURE_ERROR)

    expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
      assertEquals(
        transactionStatusDto,
        viewArgumentCaptor.allValues[idx].status,
        "Unexpected view status update at idx: $idx")
    }
    viewArgumentCaptor.allValues.last().let {
      assertNull(it.closureErrorData!!.errorDescription)
      assertNull(it.closureErrorData!!.httpErrorCode)
      assertEquals(ErrorType.COMMUNICATION_ERROR, it.closureErrorData!!.errorType)
    }
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    assertEquals(1, retryCountCaptor.value)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))

    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer should not perform refund transaction with no attempts left`() = runTest {
    Hooks.onOperatorDebug()

    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")

    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(RuntimeException("Nodo error"))
    given(deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any(), any()))
      .willReturn(mono {})

    given(
        closureRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(
        Mono.error(
          NoRetryAttemptsLeftException(
            eventCode = TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT.toString(),
            transactionId = TransactionId(UUID.randomUUID()))))
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())
    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))
    viewArgumentCaptor.allValues
      .filter { it.status == TransactionStatusDto.CLOSURE_ERROR }
      .forEach {
        assertNull(it.closureErrorData!!.errorDescription)
        assertNull(it.closureErrorData!!.httpErrorCode)
        assertEquals(ErrorType.COMMUNICATION_ERROR, it.closureErrorData!!.errorType)
      }
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer should not perform refund transaction with no attempts left with no transactions-view update if ff disabled`() =
    runTest {
      Hooks.onOperatorDebug()

      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")

      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(RuntimeException("Nodo error"))
      given(deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any(), any()))
        .willReturn(mono {})

      given(
          closureRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(
          Mono.error(
            NoRetryAttemptsLeftException(
              eventCode = TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT.toString(),
              transactionId = TransactionId(UUID.randomUUID()))))
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer does not perform refund transaction for generic error in retry event enqueue`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosureErrorEventStoreRepository.save(any())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(RuntimeException("Nodo error"))

      given(
          closureRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.error(RuntimeException("Error enqueuing retry event")))

      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(1))
        .save(argThat { it -> (it as Transaction).status == TransactionStatusDto.CLOSURE_ERROR })
      verify(closureRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))

      viewArgumentCaptor.allValues.last().let {
        assertNull(it.closureErrorData!!.errorDescription)
        assertNull(it.closureErrorData!!.httpErrorCode)
        assertEquals(ErrorType.COMMUNICATION_ERROR, it.closureErrorData!!.errorType)
      }

      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer does not perform refund transaction for generic error in retry event enqueue with no transactions-view if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosureErrorEventStoreRepository.save(any())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(RuntimeException("Nodo error"))

      given(
          closureRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.error(RuntimeException("Error enqueuing retry event")))

      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))

      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare close message correctly with OK closure outcome`() = runTest {
    Hooks.onOperatorDebug()

    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")

    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent, cancelRequestEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))

    val expectedUpdatedTransactionCanceled =
      transactionDocument(
        TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionId = TransactionId(TRANSACTION_ID)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.canceled(
            QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()
    expectedUpdatedTransactionCanceled.lastProcessedEventAt =
      ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
        .toInstant()
        .toEpochMilli()
    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
      .deleteById(any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.OK,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        UserCancelClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
          Transaction.ClientId.CHECKOUT,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.OK.toString(), Optional.empty())))
    verify(transactionTracing, times(1))
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, times(1))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer processes bare close message correctly with OK closure outcome with no transactions-view if ff disabled`() =
    runTest {
      Hooks.onOperatorDebug()

      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")

      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val expectedUpdatedTransactionCanceled =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.canceled(
              QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
        .deleteById(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          UserCancelClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            Transaction.ClientId.CHECKOUT,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, times(1))
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare close message correctly with KO closure outcome`() = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent, cancelRequestEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))
    transactionDocument.lastProcessedEventAt =
      ZonedDateTime.parse(activationEvent.creationDate).toInstant().toEpochMilli()

    val expectedUpdatedTransactionCanceled =
      transactionDocument(
        TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))
    val transactionId = TransactionId(TRANSACTION_ID)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
    doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.canceled(
            QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()
    expectedUpdatedTransactionCanceled.lastProcessedEventAt =
      ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
        .toInstant()
        .toEpochMilli()
    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
      .deleteById(any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        UserCancelClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
          Transaction.ClientId.CHECKOUT,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(), Optional.empty())))
    verify(transactionTracing, times(1))
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, times(1))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer processes bare close message correctly with KO closure outcome with no transactions-view update if feature flag disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val expectedUpdatedTransactionCanceled =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.canceled(
              QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
        .deleteById(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          UserCancelClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            Transaction.ClientId.CHECKOUT,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, times(1))
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer receive error from close payment and send a retry event`() = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent, cancelRequestEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))

    val expectedUpdatedTransactionCanceled =
      transactionDocument(
        TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionId = TransactionId(TRANSACTION_ID)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
      .willThrow(RuntimeException("Nodo error"))

    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }

    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(closureRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.canceled(
            QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
      .deleteById(any())
    // mocking
    verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
      TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        UserCancelClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          Transaction.ClientId.CHECKOUT,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[N/A] - descr:[N/A]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer receive unrecoverable error (400 Bad Request) error from close payment and do not send a retry event`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val expectedUpdatedTransactionCanceled =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
        .willThrow(ClosePaymentErrorResponseException(HttpStatus.BAD_REQUEST, ErrorDto()))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(closureRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.canceled(
              QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), eq(MOCK_TRACING_INFO), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
        TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          UserCancelClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            Transaction.ClientId.CHECKOUT,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[400] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer receive unrecoverable error (400 Bad Request) error from close payment and do not send a retry event with no transactions-view update if feature flag disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val expectedUpdatedTransactionCanceled =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
        .willThrow(ClosePaymentErrorResponseException(HttpStatus.BAD_REQUEST, ErrorDto()))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(closureRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.canceled(
              QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), eq(MOCK_TRACING_INFO), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
        TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          UserCancelClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            Transaction.ClientId.CHECKOUT,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[400] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer receive unrecoverable error (404 Not found) error from close payment and do not send a retry event`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
        .willThrow(ClosePaymentErrorResponseException(HttpStatus.BAD_REQUEST, ErrorDto()))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(closureRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.canceled(
              QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), eq(MOCK_TRACING_INFO), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
        TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          UserCancelClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            Transaction.ClientId.CHECKOUT,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[400] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer receive unrecoverable error (404 Not found) error from close payment and do not send a retry event with no transactions-view update if feature flag disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO))
        .willThrow(ClosePaymentErrorResponseException(HttpStatus.BAD_REQUEST, ErrorDto()))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(closureRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.canceled(
              QueueEvent(cancelRequestEvent as TransactionUserCanceledEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1))
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), eq(MOCK_TRACING_INFO), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
        TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          UserCancelClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            Transaction.ClientId.CHECKOUT,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[400] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes closure error message correctly with OK closure outcome for authorization completed transaction with REDIRECT gateway`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
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
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */
      Hooks.onOperatorDebug()

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      expectedUpdatedTransaction.lastProcessedEventAt =
        ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
          .toInstant()
          .toEpochMilli()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(TransactionStatusDto.CLOSED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes closure error message correctly with OK closure outcome for authorization completed transaction with REDIRECT gateway with no transactions-view update if feature flag disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
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
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */
      Hooks.onOperatorDebug()
      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes closure error message correctly with KO closure outcome for unauthorized transaction with REDIRECT gateway`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
          redirectTransactionGatewayAuthorizationRequestedData())
          as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.KO, "errorCode"))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */
      Hooks.onOperatorDebug()
      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      expectedUpdatedTransaction.lastProcessedEventAt =
        ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
          .toInstant()
          .toEpochMilli()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(TransactionStatusDto.UNAUTHORIZED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, times(1))
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes closure error message correctly with KO closure outcome for unauthorized transaction with REDIRECT gateway with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
          redirectTransactionGatewayAuthorizationRequestedData())
          as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.KO, "errorCode"))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */
      Hooks.onOperatorDebug()
      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.KO)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, times(1))
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, times(1))
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response with http error code 422 and error description Node did not receive RPT yet for transaction in closure error state`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
            errorResponse = ErrorDto().outcome("KO").description("Node did not receive RPT yet")))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(2)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(TransactionStatusDto.CLOSURE_ERROR, TransactionStatusDto.REFUND_REQUESTED)
      val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }

      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }

      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of("HTTP code:[422] - descr:[Node did not receive RPT yet]"))))
      viewArgumentCaptor.allValues
        .filter { it.status == TransactionStatusDto.CLOSURE_ERROR }
        .forEach {
          assertEquals("Node did not receive RPT yet", it.closureErrorData!!.errorDescription)
          assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, it.closureErrorData!!.httpErrorCode)
          assertEquals(ErrorType.KO_RESPONSE_RECEIVED, it.closureErrorData!!.errorType)
        }
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer refunds authorized transaction and close payment response with http code 422 and error description Node did not receive RPT yet for transaction in closure error state without transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
            errorResponse = ErrorDto().outcome("KO").description("Node did not receive RPT yet")))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(TransactionStatusDto.CLOSURE_ERROR, TransactionStatusDto.REFUND_REQUESTED)
      val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)

      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }

      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of("HTTP code:[422] - descr:[Node did not receive RPT yet]"))))
      viewArgumentCaptor.allValues
        .filter { it.status == TransactionStatusDto.CLOSURE_ERROR }
        .forEach {
          assertEquals("Node did not receive RPT yet", it.closureErrorData!!.errorDescription)
          assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, it.closureErrorData!!.httpErrorCode)
          assertEquals(ErrorType.KO_RESPONSE_RECEIVED, it.closureErrorData!!.errorType)
        }
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @ParameterizedTest
  @ValueSource(strings = ["unexpected error", "Outcome already acquired"])
  @NullSource
  fun `consumer does not perform refund for authorized transaction and close payment response with http error code 422 and error not expected description for transaction in closure error state`(
    nodeErrorDescription: String?,
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
          errorResponse = ErrorDto().outcome("KO").description(nodeErrorDescription)))

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[422] - descr:[${nodeErrorDescription ?: "N/A"}]"))))
    viewArgumentCaptor.allValues
      .filter { it.status == TransactionStatusDto.CLOSURE_ERROR }
      .forEach {
        assertEquals(nodeErrorDescription, it.closureErrorData!!.errorDescription)
        assertEquals(HttpStatus.valueOf(422), it.closureErrorData!!.httpErrorCode)
        assertEquals(ErrorType.KO_RESPONSE_RECEIVED, it.closureErrorData!!.errorType)
      }
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @ValueSource(strings = ["unexpected error", "Outcome already acquired"])
  @NullSource
  fun `consumer do not refunds authorized transaction and close payment response with http code 422 and error not expected description for trans in closure error state with no transactions-view update if ff disabled`(
    nodeErrorDescription: String?,
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
          errorResponse = ErrorDto().outcome("KO").description(nodeErrorDescription)))

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[422] - descr:[${nodeErrorDescription ?: "N/A"}]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @ValueSource(strings = ["unexpected error", "Outcome already acquired"])
  @NullSource
  fun `consumer does not perform refund for authorized transaction and close payment response with http error code 422 and error not expected description updating transaction to CLOSURE_ERROR status`(
    nodeErrorDescription: String?
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
          errorResponse = ErrorDto().outcome("KO").description(nodeErrorDescription)))

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.requested(
            QueueEvent(
              closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[422] - descr:[${nodeErrorDescription ?: "N/A"}]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @ValueSource(strings = ["unexpected error", "Outcome already acquired"])
  @NullSource
  fun `consumer do not refunds authorized transaction and close payment response with http code 422 and error not expected description updating trans to CLOSURE_ERROR status with no transactions-view update if ff disabled`(
    nodeErrorDescription: String?
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
          errorResponse = ErrorDto().outcome("KO").description(nodeErrorDescription)))

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.requested(
            QueueEvent(
              closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[422] - descr:[${nodeErrorDescription ?: "N/A"}]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @ValueSource(ints = [400, 404])
  fun `consumer should stop retry and perform refund for Node closePayment responses with http code 4xx for transaction in closure error state`(
    nodeHttpErrorCode: Int
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        refundQueueAsyncClient.sendMessageWithResponse(
          any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.valueOf(nodeHttpErrorCode),
          errorResponse = ErrorDto().outcome("KO").description("some error description")))

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsViewRepository, Mockito.times(2)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$nodeHttpErrorCode] - descr:[some error description]"))))
    verify(refundQueueAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
    val expectedViewUpdateStatuses =
      listOf(
        TransactionStatusDto.CLOSURE_ERROR,
        TransactionStatusDto.REFUND_REQUESTED,
      )
    val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
    expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
      assertEquals(
        transactionStatusDto,
        viewArgumentCaptor.allValues[idx].status,
        "Unexpected view status update at idx: $idx")
    }
    expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
      assertEquals(
        transactionEventCode,
        TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
        "Unexpected event at idx: $idx")
    }
    viewArgumentCaptor.allValues
      .filter { it.status == TransactionStatusDto.CLOSURE_ERROR }
      .forEach {
        assertEquals("some error description", it.closureErrorData!!.errorDescription)
        assertEquals(HttpStatus.valueOf(nodeHttpErrorCode), it.closureErrorData!!.httpErrorCode)
        assertEquals(ErrorType.KO_RESPONSE_RECEIVED, it.closureErrorData!!.errorType)
      }
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @ValueSource(ints = [400, 404])
  fun `consumer should stop retry and perform refund for Node closePayment responses with http code 4xx for transaction in closure error state with no transactions-view update if ff disabled`(
    nodeHttpErrorCode: Int
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        refundQueueAsyncClient.sendMessageWithResponse(
          any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.valueOf(nodeHttpErrorCode),
          errorResponse = ErrorDto().outcome("KO").description("some error description")))

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$nodeHttpErrorCode] - descr:[some error description]"))))
    verify(refundQueueAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
    val expectedViewUpdateStatuses =
      listOf(
        TransactionStatusDto.CLOSURE_ERROR,
        TransactionStatusDto.REFUND_REQUESTED,
      )
    val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
    expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
      assertEquals(
        transactionEventCode,
        TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
        "Unexpected event at idx: $idx")
    }
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  companion object {

    @JvmStatic
    fun nodeErrorResponsesForEnqueueRetryTest(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.INTERNAL_SERVER_ERROR,
            errorResponse = ErrorDto().outcome("KO").description("Internal Server Error")),
          ClosureErrorData(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "Internal Server Error",
            ErrorType.KO_RESPONSE_RECEIVED)),
        Arguments.of(
          ClosePaymentErrorResponseException(statusCode = null, errorResponse = null),
          ClosureErrorData(null, null, ErrorType.COMMUNICATION_ERROR)),
        Arguments.of(
          RuntimeException("Unexpected error while communicating with Nodo"),
          ClosureErrorData(null, null, ErrorType.COMMUNICATION_ERROR)))
  }

  @ParameterizedTest
  @MethodSource("nodeErrorResponsesForEnqueueRetryTest")
  fun `consumer should write closure retry event for Node close payment error response code 5xx for transaction in closure error state`(
    throwable: Throwable,
    expectedClosureErrorData: ClosureErrorData
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)
    val (expectedHttpErrorCode, expectedErrorDescription) =
      if (throwable is ClosePaymentErrorResponseException) {
        Pair(throwable.statusCode?.value() ?: "N/A", throwable.errorResponse?.description ?: "N/A")
      } else {
        Pair("N/A", "N/A")
      }
    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(throwable)

    given(
        closureRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())

    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(0, retryCountCaptor.value)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$expectedHttpErrorCode] - descr:[$expectedErrorDescription]"))))
    viewArgumentCaptor.allValues.last().let {
      assertEquals(expectedClosureErrorData, it.closureErrorData)
    }
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @MethodSource("nodeErrorResponsesForEnqueueRetryTest")
  fun `consumer should write closure retry event for Node close payment error response code 5xx for transaction in closure error state with no transactions-view update if ff disabled`(
    throwable: Throwable,
    expectedClosureErrorData: ClosureErrorData
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)
    val (expectedHttpErrorCode, expectedErrorDescription) =
      if (throwable is ClosePaymentErrorResponseException) {
        Pair(throwable.statusCode?.value() ?: "N/A", throwable.errorResponse?.description ?: "N/A")
      } else {
        Pair("N/A", "N/A")
      }
    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(throwable)

    given(
        closureRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())

    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(0, retryCountCaptor.value)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$expectedHttpErrorCode] - descr:[$expectedErrorDescription]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response with http error code 422 and error description Node did not receive RPT yet for transaction in closure requested state`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
            errorResponse = ErrorDto().outcome("KO").description("Node did not receive RPT yet")))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(2)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(
          TransactionStatusDto.CLOSURE_ERROR,
          TransactionStatusDto.REFUND_REQUESTED,
        )
      val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }

      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
        closureErrorEventStoreRepositoryCaptor.value.eventCode)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of("HTTP code:[422] - descr:[Node did not receive RPT yet]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer refunds authorized trans and close payment response with http code 422 and error description Node did not receive RPT yet for transaction in closure requested state with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
            errorResponse = ErrorDto().outcome("KO").description("Node did not receive RPT yet")))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(
          TransactionStatusDto.CLOSURE_ERROR,
          TransactionStatusDto.REFUND_REQUESTED,
        )
      val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
        closureErrorEventStoreRepositoryCaptor.value.eventCode)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of("HTTP code:[422] - descr:[Node did not receive RPT yet]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer does not perform refund for authorized transaction and close payment response with http error code 422 and error not expected description for transaction in closure requested state`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
            errorResponse = ErrorDto().outcome("KO").description("unknown error")))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(1)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
        closureErrorEventStoreRepositoryCaptor.value.eventCode)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of("HTTP code:[422] - descr:[unknown error]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer do not refunds authorized transaction and close payment response with http code 422 and error not expected description for trans in closure requested state with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY,
            errorResponse = ErrorDto().outcome("KO").description("unknown error")))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
        closureErrorEventStoreRepositoryCaptor.value.eventCode)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of("HTTP code:[422] - descr:[unknown error]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer does not perform refund for authorized transaction and close payment response with http error code 422 and no error response DTO in closure requested state`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY, errorResponse = null))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(1)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
        closureErrorEventStoreRepositoryCaptor.value.eventCode)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[422] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer performs refund for authorized trans and close payment response with http code 422 and no error response DTO in closure requested state with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willThrow(
          ClosePaymentErrorResponseException(
            statusCode = HttpStatus.UNPROCESSABLE_ENTITY, errorResponse = null))

      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
        closureErrorEventStoreRepositoryCaptor.value.eventCode)
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.of("HTTP code:[422] - descr:[N/A]"))))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @ParameterizedTest
  @ValueSource(ints = [400, 404])
  fun `consumer should stop retry and perform refund for Node closePayment responses with http code 4xx for transaction in closure requested state`(
    nodeHttpErrorCode: Int
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.valueOf(nodeHttpErrorCode),
          errorResponse = ErrorDto().outcome("KO").description("some error description")))
    given(
        refundQueueAsyncClient.sendMessageWithResponse(
          any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.requested(
            QueueEvent(
              closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsViewRepository, Mockito.times(2)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(refundQueueAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$nodeHttpErrorCode] - descr:[some error description]"))))
    val expectedViewUpdateStatuses =
      listOf(
        TransactionStatusDto.CLOSURE_ERROR,
        TransactionStatusDto.REFUND_REQUESTED,
      )
    val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
    expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
      assertEquals(
        transactionStatusDto,
        viewArgumentCaptor.allValues[idx].status,
        "Unexpected view status update at idx: $idx")
    }
    expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
      assertEquals(
        transactionEventCode,
        TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
        "Unexpected event at idx: $idx")
    }
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @ValueSource(ints = [400, 404])
  fun `consumer should stop retry and perform refund for Node closePayment responses with http code 4xx for transaction in closure requested state with no transactions-view update if ff disabled`(
    nodeHttpErrorCode: Int
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.valueOf(nodeHttpErrorCode),
          errorResponse = ErrorDto().outcome("KO").description("some error description")))
    given(
        refundQueueAsyncClient.sendMessageWithResponse(
          any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())

    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.requested(
            QueueEvent(
              closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(refundQueueAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$nodeHttpErrorCode] - descr:[some error description]"))))
    val expectedViewUpdateStatuses =
      listOf(
        TransactionStatusDto.CLOSURE_ERROR,
        TransactionStatusDto.REFUND_REQUESTED,
      )
    val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
    expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
      assertEquals(
        transactionEventCode,
        TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
        "Unexpected event at idx: $idx")
    }
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @MethodSource("nodeErrorResponsesForEnqueueRetryTest")
  fun `consumer should write closure retry event for Node close payment error response code 5xx for transaction in closure requested state`(
    throwable: Throwable
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(throwable)

    given(
        closureRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    val (expectedHttpErrorCode, expectedErrorDescription) =
      if (throwable is ClosePaymentErrorResponseException) {
        Pair(throwable.statusCode?.value() ?: "N/A", throwable.errorResponse?.description ?: "N/A")
      } else {
        Pair("N/A", "N/A")
      }
    /* test */
    Hooks.onOperatorDebug()
    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.requested(
            QueueEvent(
              closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())

    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(0, retryCountCaptor.value)
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$expectedHttpErrorCode] - descr:[$expectedErrorDescription]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @MethodSource("nodeErrorResponsesForEnqueueRetryTest")
  fun `consumer should write closure retry event for Node close payment error response code 5xx for transaction in closure requested state with no transactions-view update if feature flag disabled`(
    throwable: Throwable
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willThrow(throwable)

    given(
        closureRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    val (expectedHttpErrorCode, expectedErrorDescription) =
      if (throwable is ClosePaymentErrorResponseException) {
        Pair(throwable.statusCode?.value() ?: "N/A", throwable.errorResponse?.description ?: "N/A")
      } else {
        Pair("N/A", "N/A")
      }
    /* test */
    Hooks.onOperatorDebug()
    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.requested(
            QueueEvent(
              closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())

    verify(closureRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    assertEquals(0, retryCountCaptor.value)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.name,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.PROCESSING_ERROR,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          Transaction.ClientId.CHECKOUT,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.KO.toString(),
            Optional.of("HTTP code:[$expectedHttpErrorCode] - descr:[$expectedErrorDescription]"))))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response outcome KO for transaction in closure requested status`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(2)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedViewUpdateStatuses =
        listOf(TransactionStatusDto.CLOSED, TransactionStatusDto.REFUND_REQUESTED)
      val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response outcome KO for transaction in closure requested status with no transactions-view update if feature flag disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          refundQueueAsyncClient.sendMessageWithResponse(
            any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(refundQueueAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<TransactionRefundRequestedEvent>>(), any(), any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())

      val expectedEventsCodes = listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          TransactionEventCode.valueOf(refundedEventStoreRepositoryCaptor.allValues[idx].eventCode),
          "Unexpected event at idx: $idx")
      }
      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            false,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer enqueue retry event in case of error processing the received event for transaction in closure requested status`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture()))
        .willReturn(Mono.error(RuntimeException("Error updating view")))
      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          closureRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */
      Hooks.onOperatorDebug()
      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.requested(
              QueueEvent(
                closureRequestedEvent as TransactionClosureRequestedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosureErrorEventStoreRepository, Mockito.times(1)).save(any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(2)).save(any())

      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionClosureRequestedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      val expectedViewUpdateStatuses =
        listOf(
          TransactionStatusDto.CLOSED,
          TransactionStatusDto.CLOSURE_ERROR,
        )

      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
        TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
      verify(updateTransactionStatusTracerUtils, times(0)).traceStatusUpdateOperation(any())
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction performed with an onboarded method`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("true")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent(
          npgTransactionGatewayAuthorizationRequestedData(paypalWalletInfo()))
          as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      expectedUpdatedTransaction.lastProcessedEventAt =
        ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
          .toInstant()
          .toEpochMilli()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      assertEquals(TransactionStatusDto.CLOSED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)

      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            true,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction performed with an onboarded method with no transactions-view update if ff disabled`() =
    runTest {
      whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
        .thenReturn("false")
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent(
          npgTransactionGatewayAuthorizationRequestedData(paypalWalletInfo()))
          as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompleteEvent,
          closureRequestedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
            TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
          .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
      /* test */

      StepVerifier.create(
          closePaymentHelper.closePayment(
            ClosePaymentEvent.errored(
              QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
            checkpointer,
            EmptyTransaction()))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(closureRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
        .deleteById(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)

      verify(updateTransactionStatusTracerUtils, times(1))
        .traceStatusUpdateOperation(
          ClosePaymentNodoStatusUpdate(
            UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
            PSP_ID,
            PAYMENT_TYPE_CODE,
            Transaction.ClientId.CHECKOUT,
            true,
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.OK.toString(), Optional.empty())))
      verify(transactionTracing, never())
        .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @ParameterizedTest
  @EnumSource(ClientId::class)
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction tracing client`(
    clientId: ClientId
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val activationEvent =
      transactionActivateEvent().apply { this.data.clientId = clientId } as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    val expectedUpdatedTransaction =
      transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
        .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    expectedUpdatedTransaction.lastProcessedEventAt =
      ZonedDateTime.parse(closedEventStoreRepositoryCaptor.value.creationDate)
        .toInstant()
        .toEpochMilli()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    assertEquals(TransactionStatusDto.CLOSED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.OK,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)

    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          clientId,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.OK.toString(), Optional.empty())))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @ParameterizedTest
  @EnumSource(ClientId::class)
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction tracing client with no transactions-view update if ff disabled`(
    clientId: ClientId
  ) = runTest {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val activationEvent =
      transactionActivateEvent().apply { this.data.clientId = clientId } as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureErrorEvent)

    val expectedUpdatedTransaction =
      transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))
        .apply { this.sendPaymentResultOutcome = TransactionUserReceiptData.Outcome.NOT_RECEIVED }

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    doNothing().`when`(updateTransactionStatusTracerUtils).traceStatusUpdateOperation(any())
    /* test */

    StepVerifier.create(
        closePaymentHelper.closePayment(
          ClosePaymentEvent.errored(
            QueueEvent(closureErrorEvent as TransactionClosureErrorEvent, MOCK_TRACING_INFO)),
          checkpointer,
          EmptyTransaction()))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentOutcome.OK)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
    verify(closureRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(reactivePaymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never())
      .deleteById(any())
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.OK,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)

    verify(updateTransactionStatusTracerUtils, times(1))
      .traceStatusUpdateOperation(
        ClosePaymentNodoStatusUpdate(
          UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome.OK,
          PSP_ID,
          PAYMENT_TYPE_CODE,
          clientId,
          false,
          UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
            ClosePaymentOutcome.OK.toString(), Optional.empty())))
    verify(transactionTracing, never())
      .addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, never())
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
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
