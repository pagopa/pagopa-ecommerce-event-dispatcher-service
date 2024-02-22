package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.WorkflowStateDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.transactionauthrequests.v1.dto.OutcomeNpgGatewayDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionStatusDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.http.HttpStatus
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionAuthorizationRequestedRetryQueueConsumerTest {

  private val transactionsServiceClient: TransactionsServiceClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService =
    mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()

  private val checkpointer: Checkpointer = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionAuthorizationCompletedStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionAuthorizationCompletedData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val transactionAuthorizationRequestedRetryQueueConsumer =
    TransactionAuthorizationRequestedRetryQueueConsumer(
      transactionsServiceClient = transactionsServiceClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
      authorizationStateRetrieverService = authorizationStateRetrieverService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2)

  companion object {
    @JvmStatic
    fun `Recover transaction status timestamp method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of("2024-01-01T00:00:00", OffsetDateTime.parse("2024-01-01T00:00:00+01:00")),
        Arguments.of("2024-08-01T00:00:00", OffsetDateTime.parse("2024-08-01T00:00:00+02:00")))

    @JvmStatic
    fun `Patch auth request transaction service exception 4xx method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          PatchAuthRequestErrorResponseException(
            TransactionId(TRANSACTION_ID), HttpStatus.BAD_REQUEST, "error test")),
        Arguments.of(
          UnauthorizedPatchAuthorizationRequestException(
            TransactionId(TRANSACTION_ID), HttpStatus.UNAUTHORIZED)),
        Arguments.of(TransactionNotFound(TransactionId(TRANSACTION_ID).uuid)))

    @JvmStatic
    fun `Patch auth request transaction service exception 5xx method source`(): Stream<Arguments> =
      Stream.of(Arguments.of(GatewayTimeoutException()), Arguments.of(BadGatewayException("test")))
  }

  @ParameterizedTest
  @MethodSource("Recover transaction status timestamp method source")
  fun `messageReceiver consume event correctly and receive PAYMENT_COMPLETE outcome from NPG`(
    receivedOperationTime: String,
    expectedOperationTime: OffsetDateTime
  ) = runTest {
    val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val authorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())

    val authorizationRequestedRetriedEvent = transactionAuthorizationRequestedRetriedEvent(1)
    val transactionId = TransactionId(TRANSACTION_ID)
    val operationId = "operationId"
    val orderId = "orderId"
    val authorizationCode = "123456"
    val rrn = "rrn"
    val paymentEndToEndId = "paymentEndToEndId"
    val npgStateResponse =
      StateResponseDto()
        .state(WorkflowStateDto.PAYMENT_COMPLETE)
        .operation(
          OperationDto()
            .operationId(operationId)
            .orderId(orderId)
            .operationResult(OperationResultDto.EXECUTED)
            .paymentEndToEndId(paymentEndToEndId)
            .operationTime(receivedOperationTime)
            .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = authorizationCode
            this.paymentEndToEndId = paymentEndToEndId
            this.rrn = rrn
          }
        timestampOperation = expectedOperationTime
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
          authorizationRequestedRetriedEvent as TransactionEvent<Any>,
        ))

    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
              .AUTHORIZATION_REQUESTED,
            ZonedDateTime.now())))

    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
    /* test */
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(authorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(authorizationStateRetrieverRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
  }

  @Test
  fun `messageReceiver consume event correctly and receive GDI_VERIFICATION outcome from NPG`() =
    runTest {
      val receivedOperationTime = "2024-01-01T00:00:00"
      val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
      val authorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())

      val authorizationRequestedRetriedEvent = transactionAuthorizationRequestedRetriedEvent(0)
      val transactionId = TransactionId(TRANSACTION_ID)
      val operationId = "operationId"
      val orderId = "orderId"
      val authorizationCode = "123456"
      val rrn = "rrn"
      val paymentEndToEndId = "paymentEndToEndId"
      val npgStateResponse =
        StateResponseDto()
          .state(WorkflowStateDto.GDI_VERIFICATION)
          .operation(
            OperationDto()
              .operationId(operationId)
              .orderId(orderId)
              .operationResult(OperationResultDto.EXECUTED)
              .paymentEndToEndId(paymentEndToEndId)
              .operationTime(receivedOperationTime)
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
      val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID

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
            authorizationRequestedRetriedEvent as TransactionEvent<Any>,
          ))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          authorizationStateRetrieverRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(
              it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
                .AUTHORIZATION_REQUESTED,
              ZonedDateTime.now())))

      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
      /* test */
      StepVerifier.create(
          transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
            QueueEvent(authorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(authorizationStateRetrieverRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any())
      verify(authorizationStateRetrieverService, times(1))
        .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    }

  @Test
  fun `messageReceiver consume event correctly and receive GDI_VERIFICATION outcome from NPG and enqueue a new event although retry count has expired`() =
    runTest {
      val receivedOperationTime = "2024-01-01T00:00:00"
      val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
      val authorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())

      val authorizationRequestedRetriedEvent = transactionAuthorizationRequestedRetriedEvent(4)
      val transactionId = TransactionId(TRANSACTION_ID)
      val operationId = "operationId"
      val orderId = "orderId"
      val authorizationCode = "123456"
      val rrn = "rrn"
      val paymentEndToEndId = "paymentEndToEndId"
      val npgStateResponse =
        StateResponseDto()
          .state(WorkflowStateDto.GDI_VERIFICATION)
          .operation(
            OperationDto()
              .operationId(operationId)
              .orderId(orderId)
              .operationResult(OperationResultDto.EXECUTED)
              .paymentEndToEndId(paymentEndToEndId)
              .operationTime(receivedOperationTime)
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
      val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID

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
            authorizationRequestedRetriedEvent as TransactionEvent<Any>,
          ))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          authorizationStateRetrieverRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(
              it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
                .AUTHORIZATION_REQUESTED,
              ZonedDateTime.now())))

      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
      /* test */
      StepVerifier.create(
          transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
            QueueEvent(authorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(authorizationStateRetrieverRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any())
      verify(authorizationStateRetrieverService, times(1))
        .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = transactionId,
              transactionEventCode =
                TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    }

  @Test
  fun `messageReceiver consume event correctly but found transaction in authorization completed`() =
    runTest {
      val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
      val authorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())

      val authorizationRequestedRetriedEvent = transactionAuthorizationRequestedRetriedEvent(0)
      val authorizationCompleted =
        transactionAuthorizationCompletedEvent(NpgTransactionGatewayAuthorizationData())
      val transactionId = TransactionId(TRANSACTION_ID)
      val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID

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
            authorizationRequestedRetriedEvent as TransactionEvent<Any>,
            authorizationCompleted as TransactionEvent<Any>,
          ))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          authorizationStateRetrieverRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(
              it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
                .AUTHORIZATION_COMPLETED,
              ZonedDateTime.now())))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */
      StepVerifier.create(
          transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
            QueueEvent(authorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(authorizationStateRetrieverRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any())
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any())
    }

  @Test
  fun `Should enqueue retry event for 5xx error retrieving authorization status from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationRequestedRetriedEvent =
      transactionAuthorizationRequestedRetriedEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedRetriedEvent as TransactionEvent<Any>)
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(Mono.error(NpgServerErrorException("Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(
            transactionAuthorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(
                object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                jsonSerializerV2)
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = transactionId,
            transactionEventCode =
              TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
  }

  @Test
  fun `Should not enqueue retry event for 4xx error retrieving authorization status from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationRequestedRetriedEvent =
      transactionAuthorizationRequestedRetriedEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedRetriedEvent as TransactionEvent<Any>)

    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(
        Mono.error(NpgBadRequestException(transactionId, "Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(
            transactionAuthorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0)).enqueueRetryEvent(any(), eq(0), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @ParameterizedTest
  @MethodSource("Patch auth request transaction service exception 4xx method source")
  fun `Should not enqueue retry event for 4xx error performing auth request to transactions service`(
    runtimeException: RuntimeException
  ) {
    // TEST PATCH 400 -> NO retry
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationRequestedRetriedEvent =
      transactionAuthorizationRequestedRetriedEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedRetriedEvent as TransactionEvent<Any>)
    val operationId = "operationId"
    val orderId = "orderId"
    val authorizationCode = "123456"
    val rrn = "rrn"
    val paymentEndToEndId = "paymentEndToEndId"
    val npgStateResponse =
      StateResponseDto()
        .state(WorkflowStateDto.PAYMENT_COMPLETE)
        .operation(
          OperationDto()
            .operationId(operationId)
            .orderId(orderId)
            .operationResult(OperationResultDto.EXECUTED)
            .paymentEndToEndId(paymentEndToEndId)
            .operationTime("2020-01-01T00:00:00")
            .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = authorizationCode
            this.paymentEndToEndId = paymentEndToEndId
            this.rrn = rrn
          }
        timestampOperation = OffsetDateTime.parse("2020-01-01T00:00:00+01:00")
      }
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(Mono.error(runtimeException))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(
            transactionAuthorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
  }

  @ParameterizedTest
  @MethodSource("Patch auth request transaction service exception 5xx method source")
  fun `Should not enqueue retry event for 5xx error performing auth request to transactions service`(
    runtimeException: Throwable
  ) {
    // TEST PATCH Body non valido -> Dead letter o retry?
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationRequestedRetriedEvent =
      transactionAuthorizationRequestedRetriedEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedRetriedEvent as TransactionEvent<Any>)
    val operationId = "operationId"
    val orderId = "orderId"
    val authorizationCode = "123456"
    val rrn = "rrn"
    val paymentEndToEndId = "paymentEndToEndId"
    val npgStateResponse =
      StateResponseDto()
        .state(WorkflowStateDto.PAYMENT_COMPLETE)
        .operation(
          OperationDto()
            .operationId(operationId)
            .orderId(orderId)
            .operationResult(OperationResultDto.EXECUTED)
            .paymentEndToEndId(paymentEndToEndId)
            .operationTime("2020-01-01T00:00:00")
            .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = authorizationCode
            this.paymentEndToEndId = paymentEndToEndId
            this.rrn = rrn
          }
        timestampOperation = OffsetDateTime.parse("2020-01-01T00:00:00+01:00")
      }
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(Mono.error(runtimeException))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(
            transactionAuthorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
  }

  @Test
  fun `Should not enqueue invalid auth request response transactions service`() {
    // TEST PATCH Body non valido -> Dead letter o retry?
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationRequestedRetriedEvent =
      transactionAuthorizationRequestedRetriedEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)
    val operationId = "operationId"
    val orderId = "orderId"
    val authorizationCode = "123456"
    val rrn = "rrn"
    val paymentEndToEndId = "paymentEndToEndId"
    val npgStateResponse =
      StateResponseDto()
        .state(WorkflowStateDto.PAYMENT_COMPLETE)
        .operation(
          OperationDto()
            .operationId(operationId)
            .orderId(orderId)
            .operationResult(OperationResultDto.EXECUTED)
            .paymentEndToEndId(paymentEndToEndId)
            .operationTime("2020-01-01T00:00:00")
            .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = authorizationCode
            this.paymentEndToEndId = paymentEndToEndId
            this.rrn = rrn
          }
        timestampOperation = OffsetDateTime.parse("2020-01-01T00:00:00+01:00")
      }
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(mono { TransactionInfoDto() })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(
            transactionAuthorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
  }

  @Test
  fun `Should not process transaction when NPG response body doesn't contain all expected fields and enqueue retry event`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationRequestedRetriedEvent =
      transactionAuthorizationRequestedRetriedEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)
    val operationId = "operationId"
    val orderId = "orderId"
    val authorizationCode = "123456"
    val rrn = "rrn"
    val paymentEndToEndId = "paymentEndToEndId"
    val npgStateResponse =
      StateResponseDto()
        .state(WorkflowStateDto.PAYMENT_COMPLETE)
        .operation(
          OperationDto()
            .operationId(operationId)
            .orderId(orderId)
            .operationResult(OperationResultDto.EXECUTED)
            .paymentEndToEndId(paymentEndToEndId)
            // .operationTime("2024-01-01T00:00:00")
            .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = authorizationCode
            this.paymentEndToEndId = paymentEndToEndId
            this.rrn = rrn
          }
        timestampOperation = OffsetDateTime.parse("2024-01-01T00:00:00+01:00")
      }
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          QueueEvent(
            transactionAuthorizationRequestedRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(transactionId, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(0))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
  }
}
