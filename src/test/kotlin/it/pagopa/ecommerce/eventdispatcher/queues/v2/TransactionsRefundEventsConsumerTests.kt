package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
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
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
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
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  @Captor
  private lateinit var refundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

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
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
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
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

      /* test */
      StepVerifier.create(
          transactionRefundedEventsConsumer.messageReceiver(
            Either.right(QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, null)),
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
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
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
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
          as TransactionEvent<Any>
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
            Either.right(
              QueueEvent(
                refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
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
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
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
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
          as TransactionEvent<Any>
      val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
            Either.right(QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, null)),
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
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
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
    shouldWriteRetryEvent: Boolean,
    expectedWrittenEventStatus: TransactionEventCode
  ) = runTest {
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
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val gatewayClientResponse = VposDeleteResponseDto().apply { status = pgsStatus }

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
          Either.right(
            QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
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
          if (shouldWriteRetryEvent) {
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
    shouldWriteRetryEvent: Boolean,
    expectedWrittenEventStatus: TransactionEventCode
  ) = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
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
    verify(paymentGatewayClient, Mockito.times(1))
      .requestXPayRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(
        refundRetryService,
        times(
          if (shouldWriteRetryEvent) {
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
      transactionAuthorizationCompletedEvent(
        PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
        as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.XPAY
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
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
    given(paymentGatewayClient.requestXPayRefund(any()))
      .willThrow(RefundNotAllowedException(UUID.randomUUID()))
    given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now())))

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
    verify(checkpointer, Mockito.times(1)).success()
    verify(paymentGatewayClient, Mockito.times(1))
      .requestXPayRefund(
        UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
    verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        any<BinaryData>(),
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode =
              TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    val storedEvent = refundEventStoreCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
      TransactionEventCode.valueOf(storedEvent.eventCode))
    assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    assertEquals(
      String(
        jsonSerializerV2.serializeToBytes(
          QueueEvent(refundRequestedEvent as TransactionRefundRequestedEvent, MOCK_TRACING_INFO)),
        StandardCharsets.UTF_8),
      String(queueArgumentCaptor.value.toBytes(), StandardCharsets.UTF_8))
  }

  @Test
  fun `consumer does not enqueue refund retry event for error 409 response from PGS (vpos)`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(
          PgsTransactionGatewayAuthorizationData(null, AuthorizationResultDto.OK))
          as TransactionEvent<Any>
      val closureRequestedEvent =
        transactionClosureRequestedEvent() as TransactionEvent<Any> as TransactionEvent<Any>
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
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willThrow(RefundNotAllowedException(UUID.randomUUID()))
      given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
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
      verify(checkpointer, Mockito.times(1)).success()
      verify(paymentGatewayClient, Mockito.times(1))
        .requestVPosRefund(
          UUID.fromString(transaction.transactionAuthorizationRequestData.authorizationRequestId))
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
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
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
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
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
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
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
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
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
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
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
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
      given(refundRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
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
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
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
        TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
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
  fun `consumer processes refund request event correctly with for redirect transaction`() =
    runTest {
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
      val closedEvent =
        transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
      val refundRequestedEvent =
        TransactionRefundRequestedEvent(
          TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED))
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
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsRefundedEventStoreRepository.save(refundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(refundService.requestRedirectRefund(any(), any(), any(), any()))
        .willReturn(Mono.just(refundRedirectResponse))
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
          pspTransactionId = expectedPspTransactionId,
          paymentTypeCode = expectedPaymentTypeCode,
          pspId = expectedPspId)
      verify(transactionsRefundedEventStoreRepository, Mockito.times(1)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
      val storedEvent = refundEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT.toString(), storedEvent.eventCode)
      assertEquals(TransactionStatusDto.REFUND_REQUESTED, storedEvent.data.statusBeforeRefunded)
    }
}
