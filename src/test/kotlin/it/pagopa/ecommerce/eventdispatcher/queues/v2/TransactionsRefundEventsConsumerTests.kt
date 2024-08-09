package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.*
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
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

  private val npgService: NpgService = NpgService(mock<AuthorizationStateRetrieverService>())

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val refundService: RefundService = mock()

  private val refundRetryService: RefundRetryService = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData> =
    mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  @Captor
  private lateinit var refundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRefundedData>>

  @Captor private lateinit var queueArgumentCaptor: ArgumentCaptor<BinaryData>

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()
  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

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
  fun `consumer processes refund request event for a transaction without refund requested`() =
    runTest {
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
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any(), anyOrNull())
    }

  @Test
  fun `consumer enqueue refund retry event for KO response from PGS (vpos)`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse =
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
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

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
    verify(refundService, Mockito.times(1))
      .requestNpgRefund(
        "operationId",
        UUID.randomUUID(),
        BigDecimal.valueOf(0),
        "pspId",
        "correlationId",
        NpgClient.PaymentMethod.valueOf("CARDS"))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any(), anyOrNull())

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
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID,
          TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
          as TransactionEvent<Any>

      val gatewayClientResponse =
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
        .willReturn(Mono.just(gatewayClientResponse))
      given(refundRetryService.enqueueRetryEvent(any(), any(), isNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

      /* test */

      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right(QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, null)),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(refundService, Mockito.times(1))
        .requestNpgRefund(
          "operationId",
          UUID.randomUUID(),
          BigDecimal.valueOf(0),
          "pspId",
          "correlationId",
          NpgClient.PaymentMethod.valueOf("CARDS"))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), isNull(), anyOrNull())

      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
        TransactionEventCode.valueOf(storedEvent.eventCode))
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    }

  @Test
  fun `consumer doesn't process refund request event correctly with unknown payment gateway`() =
    runTest {
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
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any(), anyOrNull())
      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT.toString(), storedEvent.eventCode)
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    }

  @Test
  fun `consumer processes refund request event correctly with npg refund`() = runTest {
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
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any(), anyOrNull())
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_REFUNDED_EVENT.toString(), storedEvent.eventCode)
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }

  @Test
  fun `consumer does not enqueue refund retry event for RefundNotAllowedException response from NPG`() =
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
        .willThrow(RefundNotAllowedException(transaction.transactionId.uuid))
      given(refundRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull()))
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
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any(), anyOrNull())
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
    }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithRefundRequested`() = runTest {
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

    val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionWithRefundRequested

    assertEquals(
      getAuthorizationCompletedData(transaction, npgService).block(),
      transactionGatewayAuthorizationData)
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithCompletedAuthorization`() =
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
    }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithClosureError`() = runTest {
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
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionExpired`() = runTest {
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
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithRequestedAuthorization`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.VPOS

      val events = listOf(activationEvent, authorizationRequestEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      assertNull(getAuthorizationCompletedData(transaction, npgService).block())
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
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any(), anyOrNull())
  }

  @ParameterizedTest
  @MethodSource("redirectClientsMappingMethodSource")
  fun `consumer processes refund request event correctly with for redirect transaction`(
    touchPoint: Transaction.ClientId,
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
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any(), anyOrNull())
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(TransactionEventCode.TRANSACTION_REFUNDED_EVENT.toString(), storedEvent.eventCode)
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
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
    given(refundRetryService.enqueueRetryEvent(any(), any(), any(), anyOrNull()))
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
    verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any(), anyOrNull())

    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
  }
}
