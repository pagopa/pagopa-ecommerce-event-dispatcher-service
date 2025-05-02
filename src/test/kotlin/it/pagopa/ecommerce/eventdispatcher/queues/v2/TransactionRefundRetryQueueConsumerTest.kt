package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRefundedData
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.reduceEvents
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionActivateEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionExpiredEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionRefundErrorEvent
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.transactionRefundRequestedEvent
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionTracing
import it.pagopa.ecommerce.eventdispatcher.utils.createDateForSecondsFromNow
import it.pagopa.ecommerce.eventdispatcher.utils.npgAuthorizedOrderResponse
import java.time.ZonedDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionRefundRetryQueueConsumerTest {

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()
  private val checkpointer: Checkpointer = mock()
  private val refundService: RefundService = mock()
  private val refundRetryService: RefundRetryService = mock()
  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  private val tracingUtils = TracingUtilsTests.getMock()
  private val transactionTracing = getTransactionTracingMock()

  private lateinit var mockOpenTelemetryUtils: OpenTelemetryUtils

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRefundedData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()
  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()
  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()
  private val refundDelayFromAuthRequestMinutes = 10L
  private val eventProcessingDelaySeconds = 10L
  private val transactionRefundRetryQueueConsumer =
    TransactionRefundRetryQueueConsumer(
      paymentGatewayClient = paymentGatewayClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      refundService = refundService,
      refundRetryService = refundRetryService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2,
      ngpService =
        NpgService(
          authorizationStateRetrieverService,
          refundDelayFromAuthRequestMinutes,
          eventProcessingDelaySeconds),
      transactionTracing = transactionTracing)

  @Test
  fun `messageReceiver consume event correctly with OK outcome from gateway`() = runTest {
    val activatedEvent =
      TransactionTestUtils.transactionActivateEvent(
        TransactionTestUtils.npgTransactionGatewayActivationData())

    val authorizationRequestedEvent = TransactionTestUtils.transactionAuthorizationRequestedEvent()

    val expiredEvent =
      TransactionTestUtils.transactionExpiredEvent(
        TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
    val refundRequestedEvent =
      TransactionTestUtils.transactionRefundRequestedEvent(
        TransactionTestUtils.reduceEvents(
          activatedEvent, authorizationRequestedEvent, expiredEvent),
        TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
    val refundErrorEvent =
      TransactionTestUtils.transactionRefundErrorEvent(
        TransactionTestUtils.reduceEvents(
          activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
    val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(0)

    val gatewayClientResponse =
      RefundResponseDto().apply {
        operationId = "operationId"
        operationTime = "operationTime"
      }

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>,
          expiredEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>,
          refundRetriedEvent as TransactionEvent<Any>,
          refundErrorEvent as TransactionEvent<Any>))

    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(
        refundRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willReturn(
        Mono.just(
          TransactionTestUtils.transactionDocument(
            TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
    given(authorizationStateRetrieverService.performGetOrder(any()))
      .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
    /* test */
    StepVerifier.create(
        transactionRefundRetryQueueConsumer.messageReceiver(
          QueueEvent(refundRetriedEvent, MOCK_TRACING_INFO), checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
    assertEquals(
      TransactionStatusDto.REFUNDED,
      transactionViewRepositoryCaptor.value.status,
      "Unexpected view status")
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
      TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.value.eventCode),
      "Unexpected event code")
    verify(refundRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(transactionTracing, times(1)).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, times(1))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `messageReceiver consume event correctly with KO outcome AUTHORIZED from gateway writing refund error event`() =
    runTest {
      val retryCount = 0
      val activatedEvent =
        TransactionTestUtils.transactionActivateEvent(
          TransactionTestUtils.npgTransactionGatewayActivationData())

      val authorizationRequestedEvent =
        TransactionTestUtils.transactionAuthorizationRequestedEvent()

      val expiredEvent =
        TransactionTestUtils.transactionExpiredEvent(
          TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
      val refundRequestedEvent =
        TransactionTestUtils.transactionRefundRequestedEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent),
          TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
      val refundErrorEvent =
        TransactionTestUtils.transactionRefundErrorEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(retryCount)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>,
            refundRequestedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            refundErrorEvent as TransactionEvent<Any>))

      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.error(BadGatewayException("mockError")))
      given(
          refundRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
        .willReturn(
          Mono.just(
            TransactionTestUtils.transactionDocument(
              TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
      given(authorizationStateRetrieverService.performGetOrder(any()))
        .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            QueueEvent(refundRetriedEvent, MOCK_TRACING_INFO), checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(refundRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(retryCount, retryCountCaptor.value)
      assertEquals(TransactionStatusDto.REFUND_ERROR, transactionViewRepositoryCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
        TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.value.eventCode))
      verify(transactionTracing, times(1))
        .addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `messageReceiver consume legacy event correctly with OK outcome from gateway`() = runTest {
    val activatedEvent =
      TransactionTestUtils.transactionActivateEvent(
        TransactionTestUtils.npgTransactionGatewayActivationData())

    val authorizationRequestedEvent = TransactionTestUtils.transactionAuthorizationRequestedEvent()

    val expiredEvent =
      TransactionTestUtils.transactionExpiredEvent(
        TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
    val refundRequestedEvent =
      TransactionTestUtils.transactionRefundRequestedEvent(
        TransactionTestUtils.reduceEvents(
          activatedEvent, authorizationRequestedEvent, expiredEvent),
        TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
    val refundErrorEvent =
      TransactionTestUtils.transactionRefundErrorEvent(
        TransactionTestUtils.reduceEvents(
          activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
    val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(0)

    val gatewayClientResponse =
      RefundResponseDto().apply {
        operationId = "operationId"
        operationTime = "operationTime"
      }

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>,
          expiredEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>,
          refundRetriedEvent as TransactionEvent<Any>,
          refundErrorEvent as TransactionEvent<Any>))

    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
      .willReturn(
        Mono.just(RefundResponseDto().operationId("operationId").operationTime("operationTime")))
    given(
        refundRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willReturn(
        Mono.just(
          TransactionTestUtils.transactionDocument(
            TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
    given(authorizationStateRetrieverService.performGetOrder(any()))
      .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
    Hooks.onOperatorDebug()
    /* test */
    StepVerifier.create(
        transactionRefundRetryQueueConsumer.messageReceiver(
          QueueEvent(refundRetriedEvent, null), checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
    assertEquals(
      TransactionStatusDto.REFUNDED,
      transactionViewRepositoryCaptor.value.status,
      "Unexpected view status")
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
      TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.value.eventCode),
      "Unexpected event code")
    verify(refundRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(transactionTracing, times(1)).addSpanAttributesRefundedFlowFromTransaction(any(), any())
    verify(mockOpenTelemetryUtils, times(1))
      .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
  }

  @Test
  fun `messageReceiver consume legacy event correctly with KO outcome AUTHORIZED from gateway writing refund error event`() =
    runTest {
      val retryCount = 0

      val activatedEvent =
        TransactionTestUtils.transactionActivateEvent(
          TransactionTestUtils.npgTransactionGatewayActivationData())

      val authorizationRequestedEvent =
        TransactionTestUtils.transactionAuthorizationRequestedEvent()

      val expiredEvent =
        TransactionTestUtils.transactionExpiredEvent(
          TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))

      val refundRequestedEvent =
        TransactionTestUtils.transactionRefundRequestedEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent),
          TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))

      val refundErrorEvent =
        TransactionTestUtils.transactionRefundErrorEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(retryCount)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>,
            refundRequestedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            refundErrorEvent as TransactionEvent<Any>))

      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.error(BadGatewayException("mockError")))
      given(
          refundRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), isNull(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
        .willReturn(
          Mono.just(
            TransactionTestUtils.transactionDocument(
              TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
      given(authorizationStateRetrieverService.performGetOrder(any()))
        .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            QueueEvent(refundRetriedEvent, null), checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(refundRetryService, times(1))
        .enqueueRetryEvent(any(), any(), isNull(), anyOrNull(), anyOrNull())
      assertEquals(retryCount, retryCountCaptor.value)
      assertEquals(TransactionStatusDto.REFUND_ERROR, transactionViewRepositoryCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
        TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.value.eventCode))
      verify(transactionTracing, times(1))
        .addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `messageReceiver consume event correctly with KO outcome AUTHORIZED from gateway not writing refund error event for retried event`() =
    runTest {
      val retryCount = 1
      val activatedEvent =
        TransactionTestUtils.transactionActivateEvent(
          TransactionTestUtils.npgTransactionGatewayActivationData())

      val authorizationRequestedEvent =
        TransactionTestUtils.transactionAuthorizationRequestedEvent()

      val expiredEvent =
        TransactionTestUtils.transactionExpiredEvent(
          TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
      val refundRequestedEvent =
        TransactionTestUtils.transactionRefundRequestedEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent),
          TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
      val refundErrorEvent =
        TransactionTestUtils.transactionRefundErrorEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(retryCount)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>,
            refundRequestedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            refundErrorEvent as TransactionEvent<Any>))

      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.error(BadGatewayException("mockError")))
      given(
          refundRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(authorizationStateRetrieverService.performGetOrder(any()))
        .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            QueueEvent(refundRetriedEvent, MOCK_TRACING_INFO), checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(refundRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(retryCount, retryCountCaptor.value)
      verify(transactionTracing, times(1))
        .addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `messageReceiver consume event aborting operation for transaction in unexpected state`() =
    runTest {
      val activatedEvent = TransactionTestUtils.transactionActivateEvent()

      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(1)

      val gatewayClientResponse =
        RefundResponseDto().apply {
          operationId = "operationId"
          operationTime = "operationTime"
        }

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
          ))

      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(
          refundRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            QueueEvent(refundRetriedEvent, MOCK_TRACING_INFO), checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(refundService, times(0)).requestNpgRefund(any(), any(), any(), any(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(refundRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionRefundRetriedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)),
        )
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  @Test
  fun `messageReceiver consume event writing event to dead letter queue because of no more attempt left for retry`() =
    runTest {
      val retryCount = 3

      val activatedEvent =
        TransactionTestUtils.transactionActivateEvent(
          TransactionTestUtils.npgTransactionGatewayActivationData())

      val authorizationRequestedEvent =
        TransactionTestUtils.transactionAuthorizationRequestedEvent()

      val expiredEvent =
        TransactionTestUtils.transactionExpiredEvent(
          TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))

      val refundRequestedEvent =
        TransactionTestUtils.transactionRefundRequestedEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent),
          TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))

      val refundErrorEvent =
        TransactionTestUtils.transactionRefundErrorEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(retryCount)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>,
            refundRequestedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            refundErrorEvent as TransactionEvent<Any>))

      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
        .willReturn(
          Mono.just(
            TransactionTestUtils.transactionDocument(
              TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.error(BadGatewayException("mockError")))
      given(
          refundRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.error(NoRetryAttemptsLeftException("No retry attempt left")))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})
      given(authorizationStateRetrieverService.performGetOrder(any()))
        .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            QueueEvent(refundRetriedEvent, MOCK_TRACING_INFO), checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(refundRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      assertEquals(retryCount, retryCountCaptor.value)
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionRefundRetriedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TransactionTestUtils.TRANSACTION_ID),
              transactionEventCode =
                TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.RETRY_EVENT_NO_ATTEMPTS_LEFT)),
        )
      verify(transactionTracing, never()).addSpanAttributesRefundedFlowFromTransaction(any(), any())
      verify(mockOpenTelemetryUtils, never())
        .addSpanWithAttributes(eq(TransactionTracing::class.simpleName), any())
    }

  //////
  @Test
  fun `messageReceiver consume event correctly with OK outcome from gateway and add a custom opentelemetry span`() =
    runTest {
      val transactionActivateEvt =
        transactionActivateEvent(TransactionTestUtils.npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvt = transactionAuthorizationRequestedEvent()
      val transactionExpiredEvt =
        transactionExpiredEvent(
          reduceEvents(transactionActivateEvt, transactionAuthorizationRequestedEvt))
      val transactionRefundRequestedEvt =
        transactionRefundRequestedEvent(
          reduceEvents(
            transactionActivateEvt, transactionAuthorizationRequestedEvt, transactionExpiredEvt),
          TransactionTestUtils.npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
      val transactionRefundErrorEvt =
        transactionRefundErrorEvent(
          reduceEvents(
            transactionActivateEvt,
            transactionAuthorizationRequestedEvt,
            transactionExpiredEvt,
            transactionRefundRequestedEvt))

      transactionActivateEvt.creationDate = createDateForSecondsFromNow(10 * 60)
      transactionAuthorizationRequestedEvt.creationDate = createDateForSecondsFromNow(8 * 60)
      transactionExpiredEvt.creationDate = createDateForSecondsFromNow(6 * 60)
      transactionRefundRequestedEvt.creationDate = createDateForSecondsFromNow(4 * 60)
      transactionRefundErrorEvt.creationDate = createDateForSecondsFromNow(2 * 60)

      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(0)

      val gatewayClientResponse =
        RefundResponseDto().apply {
          operationId = "operationId"
          operationTime = "operationTime"
        }

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            transactionActivateEvt as TransactionEvent<Any>,
            transactionAuthorizationRequestedEvt as TransactionEvent<Any>,
            transactionExpiredEvt as TransactionEvent<Any>,
            transactionRefundRequestedEvt as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            transactionRefundErrorEvt as TransactionEvent<Any>))

      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundService.requestNpgRefund(any(), any(), any(), any(), any(), any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(
          refundRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
        .willReturn(
          Mono.just(
            TransactionTestUtils.transactionDocument(
              TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
      given(authorizationStateRetrieverService.performGetOrder(any()))
        .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            QueueEvent(refundRetriedEvent, MOCK_TRACING_INFO), checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(refundService, times(1)).requestNpgRefund(any(), any(), any(), any(), any(), any())
      assertEquals(
        TransactionStatusDto.REFUNDED,
        transactionViewRepositoryCaptor.value.status,
        "Unexpected view status")
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
        TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.value.eventCode),
        "Unexpected event code")
      verify(refundRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(transactionTracing, times(1))
        .addSpanAttributesRefundedFlowFromTransaction(any(), any())
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
      assertEquals(
        "pspId", capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.PSPID)))
      assertEquals(
        null,
        capturedAttributes.get(
          AttributeKey.longKey(TransactionTracing.TRANSACTIONAUTHORIZATIONTIME)))
      assertEquals(
        TransactionStatusDto.REFUNDED.toString(),
        capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONSTATUS)))
      assertEquals(
        TransactionTestUtils.TRANSACTION_ID,
        capturedAttributes.get(AttributeKey.stringKey(TransactionTracing.TRANSACTIONID)))
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
