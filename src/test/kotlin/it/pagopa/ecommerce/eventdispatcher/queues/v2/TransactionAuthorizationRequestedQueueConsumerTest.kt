package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
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
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgBadRequestException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgServerErrorException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgStateService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.transactionauthrequests.v1.dto.OutcomeNpgGatewayDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionStatusDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TransactionAuthorizationRequestedQueueConsumerTest {

  private val transactionsServiceClient: TransactionsServiceClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService =
    mock()

  private val npgStateService: NpgStateService = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()

  private val checkpointer: Checkpointer = mock()

  private val transactionAuthorizationRequestedQueueConsumer =
    TransactionAuthorizationRequestedQueueConsumer(
      transactionsServiceClient = transactionsServiceClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
      npgStateService = npgStateService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2)

  companion object {
    @JvmStatic
    fun `Recover transaction status timestamp method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of("2024-01-01T00:00:00", OffsetDateTime.parse("2024-01-01T00:00:00+01:00")),
        Arguments.of("2024-08-01T00:00:00", OffsetDateTime.parse("2024-08-01T00:00:00+02:00")))
  }

  @ParameterizedTest
  @MethodSource("Recover transaction status timestamp method source")
  fun `Should recover transaction authorization status calling NPG get state for OK auth status`(
    receivedOperationTime: String,
    expectedOperationTime: OffsetDateTime
  ) {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
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
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(npgStateService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(1))
      .getStateNpg(transactionId.uuid, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should recover transaction authorization status calling NPG get state for KO auth status`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)
    val operationId = "operationId"
    val orderId = "orderId"
    val paymentEndToEndId = "paymentEndToEndId"
    val npgStateResponse =
      StateResponseDto()
        .state(WorkflowStateDto.PAYMENT_COMPLETE)
        .operation(
          OperationDto()
            .operationId(operationId)
            .orderId(orderId)
            .operationResult(OperationResultDto.DECLINED)
            .paymentEndToEndId(paymentEndToEndId)
            .operationTime("2024-01-01T00:00:00"))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.DECLINED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = null
            this.paymentEndToEndId = paymentEndToEndId
            this.rrn = null
          }
        timestampOperation = OffsetDateTime.parse("2024-01-01T00:00:00+01:00")
      }
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(npgStateService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(1))
      .getStateNpg(transactionId.uuid, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not process transaction in wrong state`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(transactionActivatedEvent as TransactionEvent<Any>)

    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))

    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(0)).getStateNpg(any(), any(), any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not process transaction for payment gateway different from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)

    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))

    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(0)).getStateNpg(any(), any(), any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
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
  fun `Should enqueue retry event for 5xx error retrieving authorization status from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    given(npgStateService.getStateNpg(any(), any(), any(), any()))
      .willReturn(
        Mono.error(
          NpgBadRequestException(transactionId.uuid, "Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(1))
      .getStateNpg(transactionId.uuid, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
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
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)

    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverRetryService.enqueueRetryEvent(any(), any(), any()))
      .willReturn(Mono.empty())
    given(npgStateService.getStateNpg(any(), any(), any(), any()))
      .willReturn(Mono.error(NpgServerErrorException("Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(1))
      .getStateNpg(transactionId.uuid, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1)).enqueueRetryEvent(any(), eq(1), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not enqueue retry event for error performing auth request to transactions service`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
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
    given(npgStateService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(1))
      .getStateNpg(transactionId.uuid, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not process transaction when NPG response body doesn't contain all expected fields`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
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
    given(npgStateService.getStateNpg(any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono { TransactionInfoDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED) })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    // Test
    StepVerifier.create(
        transactionAuthorizationRequestedQueueConsumer.messageReceiver(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(npgStateService, times(1))
      .getStateNpg(transactionId.uuid, expectedGetStateSessionId, PSP_ID, NPG_CORRELATION_ID)
    verify(transactionsServiceClient, times(0))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }
}
