package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationRequestedData
import it.pagopa.ecommerce.commons.documents.v2.authorization.WalletInfo
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.WorkflowStateDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.utils.NpgClientUtils.EndToEndId
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.UserStatsServiceClient
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import it.pagopa.generated.ecommerce.userstats.dto.GuestMethodLastUsageData
import it.pagopa.generated.ecommerce.userstats.dto.UserLastPaymentMethodData
import it.pagopa.generated.ecommerce.userstats.dto.WalletLastUsageData
import it.pagopa.generated.transactionauthrequests.v2.dto.OutcomeNpgGatewayDto
import it.pagopa.generated.transactionauthrequests.v2.dto.TransactionStatusDto
import it.pagopa.generated.transactionauthrequests.v2.dto.UpdateAuthorizationRequestDto
import it.pagopa.generated.transactionauthrequests.v2.dto.UpdateAuthorizationResponseDto
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
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
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.http.HttpStatus
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class AuthorizationRequestedHelperTests {

  private val transactionsServiceClient: TransactionsServiceClient = mock()

  private val userStatsServiceClient: UserStatsServiceClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService =
    mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val authRequestedQueueAsyncClient: QueueAsyncClient = mock()

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

  private val firstAttemptOffsetSeconds = 180

  private val transientQueueTTLSeconds = 300

  private val authorizationRequestedHelper =
    AuthorizationRequestedHelper(
      transactionsServiceClient = transactionsServiceClient,
      userStatsServiceClient = userStatsServiceClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
      authorizationStateRetrieverService = authorizationStateRetrieverService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2,
      enableSaveLastMethodUsage = true,
      authRequestedQueueAsyncClient = authRequestedQueueAsyncClient,
      firstAttemptDelaySeconds = firstAttemptOffsetSeconds,
      transientQueueTTLSeconds = transientQueueTTLSeconds)

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

    @JvmStatic
    fun `messageReceiver consume event correctly and perform PATCH auth request for PaymentMethod payment circuit`():
      Stream<Arguments> =
      Stream.of(
        Arguments.of(EndToEndId.BANCOMAT_PAY, NpgClient.PaymentMethod.BANCOMATPAY),
        Arguments.of(EndToEndId.MYBANK, NpgClient.PaymentMethod.MYBANK))
  }

  @ParameterizedTest
  @MethodSource("Recover transaction status timestamp method source")
  fun `messageReceiver consume event correctly and receive PAYMENT_COMPLETE outcome from NPG with card fields`(
    receivedOperationTime: String,
    expectedOperationTime: OffsetDateTime
  ) = runTest {
    val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val authorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())

    val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
          authorizationOutcomeWaitingEvent as TransactionEvent<Any>,
        ))

    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), any(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
              .AUTHORIZATION_REQUESTED,
            ZonedDateTime.now())))

    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    /* test */
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
  }

  @ParameterizedTest
  @MethodSource("Recover transaction status timestamp method source")
  fun `messageReceiver consume event correctly and receive PAYMENT_COMPLETE outcome from NPG with MyBank fields`(
    receivedOperationTime: String,
    expectedOperationTime: OffsetDateTime
  ) = runTest {
    val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val authorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())

    val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val operationId = "operationId"
    val orderId = "orderId"
    val validationServiceId = "123456"
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
            .additionalData(mapOf("validationServiceId" to validationServiceId)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.paymentEndToEndId = paymentEndToEndId
            this.validationServiceId = validationServiceId
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
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), any(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
              .AUTHORIZATION_REQUESTED,
            ZonedDateTime.now())))

    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    /* test */
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
  }

  @ParameterizedTest
  @MethodSource("Recover transaction status timestamp method source")
  fun `messageReceiver consume event correctly and receive PAYMENT_COMPLETE outcome from NPG with error fields`(
    receivedOperationTime: String,
    expectedOperationTime: OffsetDateTime
  ) = runTest {
    val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val authorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())

    val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(1)
    val transactionId = TransactionId(TRANSACTION_ID)
    val operationId = "operationId"
    val orderId = "orderId"
    val errorCode = "errorCode"
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
            .additionalData(mapOf("errorCode" to errorCode)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.paymentEndToEndId = paymentEndToEndId
            this.errorCode = errorCode
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
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), any(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
              .AUTHORIZATION_REQUESTED,
            ZonedDateTime.now())))

    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    /* test */
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
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

      val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
            authorizationRequestedEvent as TransactionEvent<Any>))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          authorizationStateRetrieverRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(
              it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
                .AUTHORIZATION_REQUESTED,
              ZonedDateTime.now())))

      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      /* test */
      StepVerifier.create(
          authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
            QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(authorizationStateRetrieverRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(authorizationStateRetrieverService, times(1))
        .getStateNpg(
          transactionId,
          expectedGetStateSessionId,
          PSP_ID,
          NPG_CORRELATION_ID,
          NpgClient.PaymentMethod.CARDS)
      verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
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

      val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(4)
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
            authorizationOutcomeWaitingEvent as TransactionEvent<Any>,
          ))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          authorizationStateRetrieverRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), anyOrNull(), anyOrNull()))
        .willReturn(Mono.empty())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(
            transactionDocument(
              it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
                .AUTHORIZATION_REQUESTED,
              ZonedDateTime.now())))

      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      /* test */
      StepVerifier.create(
          authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
            QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(authorizationStateRetrieverRetryService, times(1))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(authorizationStateRetrieverService, times(1))
        .getStateNpg(
          transactionId,
          expectedGetStateSessionId,
          PSP_ID,
          NPG_CORRELATION_ID,
          NpgClient.PaymentMethod.CARDS)
      verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
    }

  @Test
  fun `messageReceiver consume event correctly but found transaction in authorization completed`() =
    runTest {
      val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
      val authorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())

      val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
      val authorizationCompleted =
        transactionAuthorizationCompletedEvent(NpgTransactionGatewayAuthorizationData())

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
            authorizationOutcomeWaitingEvent as TransactionEvent<Any>,
            authorizationCompleted as TransactionEvent<Any>,
          ))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          authorizationStateRetrieverRetryService.enqueueRetryEvent(
            any(), retryCountCaptor.capture(), any(), any(), anyOrNull()))
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
          authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
            QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(authorizationStateRetrieverRetryService, times(0))
        .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any())
    }

  @Test
  fun `Should enqueue retry event for 5xx error retrieving authorization status from NPG for retry`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(1)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationOutcomeWaitingEvent as TransactionEvent<Any>)
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(Mono.error(NpgServerErrorException("Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not enqueue retry event for 4xx error retrieving authorization status from NPG for retry`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(1)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationOutcomeWaitingEvent as TransactionEvent<Any>)

    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(
        Mono.error(NpgBadRequestException(transactionId, "Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @ParameterizedTest
  @MethodSource("Patch auth request transaction service exception 4xx method source")
  fun `Should not enqueue retry event for 4xx error performing auth request to transactions service for retry`(
    runtimeException: RuntimeException
  ) {
    // TEST PATCH 400 -> NO retry
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationOutcomeWaitingEvent as TransactionEvent<Any>)
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(Mono.error(runtimeException))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
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
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(Mono.error(runtimeException))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
  }

  @Test
  fun `Should not enqueue invalid auth request response transactions service for retry`() {
    // TEST PATCH Body non valido -> Dead letter o retry?
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(mono { UpdateAuthorizationResponseDto() })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
  }

  @Test
  fun `Should not process transaction when NPG response body doesn't contain all expected fields and enqueue retry event for retry`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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

    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
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
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authRequestedQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should recover transaction authorization status calling NPG get state for KO auth status`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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

    Hooks.onOperatorDebug()
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
      .willReturn(queueSuccessfulResponse())

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authRequestedQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should not process transaction in wrong state after last used method is saved`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
    val transactionAuthorizationCompleted =
      transactionAuthorizationCompletedEvent(NpgTransactionGatewayAuthorizationData())
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationCompleted as TransactionEvent<Any>)

    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))

    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(0))
      .getStateNpg(any(), any(), any(), any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not process transaction in wrong state to save last used method`() {
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
        authorizationRequestedHelper.authorizationRequestedHandler(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(0))
      .getStateNpg(any(), any(), any(), any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not save user last used method for transaction in wrong state`() {
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
        authorizationRequestedHelper.authorizationRequestedHandler(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(0))
      .getStateNpg(any(), any(), any(), any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not enqueue authorization outcome waiting event for transaction for payment gateway different from NPG`() {
    // pre-conditions
    val paymentInstrumentId = UUID.randomUUID().toString()
    val authDate = Instant.now().toString()
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
    transactionAuthorizationRequestedEvent.creationDate = authDate
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
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        authorizationRequestedHelper.authorizationRequestedHandler(
          QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(0))
      .getStateNpg(any(), any(), any(), any(), any())

    val expectedUserLastPaymentMethodDataDto =
      GuestMethodLastUsageData()
        .paymentMethodId(UUID.fromString(paymentInstrumentId))
        .date(OffsetDateTime.parse(authDate)) as UserLastPaymentMethodData
    verify(userStatsServiceClient, times(1))
      .saveLastUsage(UUID.fromString(USER_ID), expectedUserLastPaymentMethodDataDto)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authRequestedQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should not process transaction for payment gateway different from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(0))
      .getStateNpg(any(), any(), any(), any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should enqueue retry event for 5xx error retrieving authorization status from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(
        Mono.error(NpgBadRequestException(transactionId, "Error retrieving transaction status")))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())

    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
  }

  @Test
  fun `Should not enqueue retry event for 4xx error retrieving authorization status from NPG`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(Mono.error(NpgServerErrorException("Error retrieving transaction status")))
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())

    // Test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1))
      .enqueueRetryEvent(any(), eq(0), any(), anyOrNull(), anyOrNull())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    /*verify(authRequestedOutcomeWaitingQueueAsyncClient, times(1))
    .sendMessageWithResponse(
        argThat<BinaryData> {
            TransactionEventCode.valueOf(
                this.toObject(
                    object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
                    jsonSerializerV2
                ).event.eventCode
            ) == TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT &&
                    this.toObject(
                        object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
                        jsonSerializerV2
                    ).event.data.retryCount == 0
        },
        eq(Duration.ofSeconds(firstAttemptOffsetSeconds.toLong())),
        eq(Duration.ofSeconds(transientQueueTTLSeconds.toLong()))
    )*/
  }

  @ParameterizedTest
  @MethodSource("Patch auth request transaction service exception 4xx method source")
  fun `Should not enqueue retry event for 4xx error performing auth request to transactions service`(
    runtimeException: java.lang.RuntimeException
  ) {
    // TEST PATCH 400 -> NO retry
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(Mono.error(runtimeException))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
  }

  @ParameterizedTest
  @MethodSource("Patch auth request transaction service exception 5xx method source")
  fun `Should not enqueue retry event for 5xx error performing auth request to transactions service for retry`(
    runtimeException: Throwable
  ) {
    // TEST PATCH Body non valido -> Dead letter o retry?
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(1)
    val transactionId = TransactionId(TRANSACTION_ID)
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
        transactionAuthorizationOutcomeWaitingEvent as TransactionEvent<Any>)
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(Mono.error(runtimeException))
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
  }

  @Test
  fun `Should not enqueue invalid auth request response transactions service`() {
    // TEST PATCH Body non valido -> Dead letter o retry?
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(mono { UpdateAuthorizationResponseDto() })
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(authRequestedQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should not process transaction when NPG response body doesn't contain all expected fields and enqueue retry event`() {
    // pre-conditions
    val transactionActivatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())
    transactionAuthorizationRequestedEvent.data.paymentInstrumentId = UUID.randomUUID().toString()
    transactionAuthorizationRequestedEvent.creationDate = Instant.now().toString()
    val transactionAuthorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(0)
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

    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId.value()))
      .willReturn(Flux.fromIterable(events))
    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), any(), any(), anyOrNull(), anyOrNull()))
      .willReturn(Mono.empty())
    given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())

    // Test
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(
            transactionAuthorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any(), any())
    verify(authorizationStateRetrieverRetryService, times(1))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
  }

  @ParameterizedTest
  @MethodSource(
    "messageReceiver consume event correctly and perform PATCH auth request for PaymentMethod payment circuit")
  fun `messageReceiver consume event correctly and perform PATCH auth request for apm payment circuit retrieving paymentEndToEndId from NPG additionalData`(
    endToEndId: EndToEndId,
    paymentMethod: NpgClient.PaymentMethod
  ) = runTest {
    val activatedEvent = transactionActivateEvent(npgTransactionGatewayActivationData())
    val authorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        npgTransactionGatewayAuthorizationRequestedData())

    val authorizationOutcomeWaitingEvent = transactionAuthorizationOutcomeWaitingEvent(1)
    val transactionId = TransactionId(TRANSACTION_ID)
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
            .operationResult(OperationResultDto.EXECUTED)
            .paymentEndToEndId(paymentEndToEndId)
            .operationTime("2024-01-01T00:00:00")
            .paymentCircuit(paymentMethod.name)
            .additionalData(mapOf(endToEndId.value to endToEndId.value)))
    val expectedGetStateSessionId = NPG_CONFIRM_PAYMENT_SESSION_ID
    val expectedPatchAuthRequest =
      UpdateAuthorizationRequestDto().apply {
        outcomeGateway =
          OutcomeNpgGatewayDto().apply {
            this.paymentGatewayType = "NPG"
            this.operationResult = OutcomeNpgGatewayDto.OperationResultEnum.EXECUTED
            this.orderId = orderId
            this.operationId = operationId
            this.authorizationCode = null
            this.paymentEndToEndId = endToEndId.value
            this.rrn = null
          }
        timestampOperation = OffsetDateTime.parse("2024-01-01T00:00:00+01:00")
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
          authorizationOutcomeWaitingEvent as TransactionEvent<Any>,
        ))

    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          any(), retryCountCaptor.capture(), any(), any(), anyOrNull()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
              .AUTHORIZATION_REQUESTED,
            ZonedDateTime.now())))

    given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
      .willReturn(mono { npgStateResponse })
    given(transactionsServiceClient.patchAuthRequest(any(), any()))
      .willReturn(
        mono {
          UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        })
    /* test */
    StepVerifier.create(
        authorizationRequestedHelper.authorizationOutcomeWaitingHandler(
          QueueEvent(authorizationOutcomeWaitingEvent, TracingInfoTest.MOCK_TRACING_INFO),
          checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(authorizationStateRetrieverRetryService, times(0))
      .enqueueRetryEvent(any(), any(), any(), anyOrNull(), anyOrNull())
    verify(authorizationStateRetrieverService, times(1))
      .getStateNpg(
        transactionId,
        expectedGetStateSessionId,
        PSP_ID,
        NPG_CORRELATION_ID,
        NpgClient.PaymentMethod.CARDS)
    verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
    verify(transactionsServiceClient, times(1))
      .patchAuthRequest(transactionId, expectedPatchAuthRequest)
  }

  @Test
  fun `messageReceiver consume event correctly and call userStatsService to save last usage with right data for guest payment`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate = Instant.now().toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      transactionAuthorizationRequestedEvent.creationDate = authDate
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
              .operationTime("2024-01-01T00:00:00")
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))

      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
      given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(checkpointer.success()).willReturn(Mono.empty())

      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      val expectedUserLastPaymentMethodDataDto =
        GuestMethodLastUsageData()
          .paymentMethodId(UUID.fromString(paymentInstrumentId))
          .date(OffsetDateTime.parse(authDate)) as UserLastPaymentMethodData
      verify(userStatsServiceClient, times(1))
        .saveLastUsage(UUID.fromString(USER_ID), expectedUserLastPaymentMethodDataDto)
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      verify(authRequestedQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            val queueWrittenEvent =
              this.toObject(
                object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                jsonSerializerV2)
            assertEquals(transactionAuthorizationRequestedEvent, queueWrittenEvent.event)
            true
          },
          argThat {
            Duration.ofSeconds(firstAttemptOffsetSeconds.toLong()).minus(this).abs().toMillis() <=
              10000
          },
          eq(Duration.ofSeconds(transientQueueTTLSeconds.toLong())))
    }

  @Test
  fun `messageReceiver consume event correctly and doesn't call userStatsService to save last usage`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      transactionActivatedEvent.data.userId = null
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate = Instant.now().toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      transactionAuthorizationRequestedEvent.creationDate = authDate
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
              .operationTime("2024-01-01T00:00:00")
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(checkpointer.success()).willReturn(Mono.empty())

      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
      verify(authRequestedQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            val queueWrittenEvent =
              this.toObject(
                object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                jsonSerializerV2)
            assertEquals(transactionAuthorizationRequestedEvent, queueWrittenEvent.event)
            true
          },
          argThat {
            Duration.ofSeconds(firstAttemptOffsetSeconds.toLong()).minus(this).abs().toMillis() <=
              10000
          },
          eq(Duration.ofSeconds(transientQueueTTLSeconds.toLong())))
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
    }

  @Test
  fun `messageReceiver consume event correctly and call userStatsService to save last usage with right data for saved wallet`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate = Instant.now().toString()
      val walletId = UUID.randomUUID().toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      (transactionAuthorizationRequestedEvent.data.transactionGatewayAuthorizationRequestedData
          as NpgTransactionGatewayAuthorizationRequestedData)
        .walletInfo = WalletInfo(walletId, null)
      transactionAuthorizationRequestedEvent.creationDate = authDate
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
              .operationTime("2024-01-01T00:00:00")
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
      given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(checkpointer.success()).willReturn(Mono.empty())

      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      val expectedUserLastPaymentMethodDataDto =
        WalletLastUsageData()
          .walletId(UUID.fromString(walletId))
          .date(OffsetDateTime.parse(authDate)) as UserLastPaymentMethodData
      verify(userStatsServiceClient, times(1))
        .saveLastUsage(UUID.fromString(USER_ID), expectedUserLastPaymentMethodDataDto)
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      verify(authRequestedQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            val queueWrittenEvent =
              this.toObject(
                object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                jsonSerializerV2)
            assertEquals(transactionAuthorizationRequestedEvent, queueWrittenEvent.event)
            true
          },
          argThat {
            Duration.ofSeconds(firstAttemptOffsetSeconds.toLong()).minus(this).abs().toMillis() <=
              10000
          },
          eq(Duration.ofSeconds(transientQueueTTLSeconds.toLong())))
    }

  @Test
  fun `messageReceiver consume event correctly and call userStatsService to save last usage but it throws error and all other invocations are not affected by this error`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate = Instant.now().toString()
      val walletId = UUID.randomUUID().toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      (transactionAuthorizationRequestedEvent.data.transactionGatewayAuthorizationRequestedData
          as NpgTransactionGatewayAuthorizationRequestedData)
        .walletInfo = WalletInfo(walletId, null)
      transactionAuthorizationRequestedEvent.creationDate = authDate
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
              .operationTime("2024-01-01T00:00:00")
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))

      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      given(userStatsServiceClient.saveLastUsage(any(), any()))
        .willReturn(Mono.error(RuntimeException("Bad Request")))
      given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(checkpointer.success()).willReturn(Mono.empty())

      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      val expectedUserLastPaymentMethodDataDto =
        WalletLastUsageData()
          .walletId(UUID.fromString(walletId))
          .date(OffsetDateTime.parse(authDate)) as UserLastPaymentMethodData
      verify(userStatsServiceClient, times(1))
        .saveLastUsage(UUID.fromString(USER_ID), expectedUserLastPaymentMethodDataDto)
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      verify(authRequestedQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            val queueWrittenEvent =
              this.toObject(
                object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                jsonSerializerV2)
            assertEquals(transactionAuthorizationRequestedEvent, queueWrittenEvent.event)
            true
          },
          argThat {
            Duration.ofSeconds(firstAttemptOffsetSeconds.toLong()).minus(this).abs().toMillis() <=
              10000
          },
          eq(Duration.ofSeconds(transientQueueTTLSeconds.toLong())))
    }

  @Test
  fun `messageReceiver consume event correctly and not call userStatsService when feature flag is disabled`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate = Instant.now().toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      transactionAuthorizationRequestedEvent.creationDate = authDate
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
              .operationTime("2024-01-01T00:00:00")
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))

      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(checkpointer.success()).willReturn(Mono.empty())
      val authorizationRequestedHelper =
        AuthorizationRequestedHelper(
          transactionsServiceClient = transactionsServiceClient,
          userStatsServiceClient = userStatsServiceClient,
          transactionsEventStoreRepository = transactionsEventStoreRepository,
          authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
          authorizationStateRetrieverService = authorizationStateRetrieverService,
          deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
          tracingUtils = tracingUtils,
          strictSerializerProviderV2 = strictJsonSerializerProviderV2,
          enableSaveLastMethodUsage = false,
          authRequestedQueueAsyncClient = authRequestedQueueAsyncClient,
          firstAttemptDelaySeconds = firstAttemptOffsetSeconds,
          transientQueueTTLSeconds = transientQueueTTLSeconds)
      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
      verify(authRequestedQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            val queueWrittenEvent =
              this.toObject(
                object : TypeReference<QueueEvent<TransactionAuthorizationRequestedEvent>>() {},
                jsonSerializerV2)
            assertEquals(transactionAuthorizationRequestedEvent, queueWrittenEvent.event)
            true
          },
          argThat {
            Duration.ofSeconds(firstAttemptOffsetSeconds.toLong()).minus(this).abs().toMillis() <=
              10000
          },
          eq(Duration.ofSeconds(transientQueueTTLSeconds.toLong())))
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
    }

  @Test
  fun `messageReceiver call userStatsService to save last usage with right data for saved wallet without performing GET state api call for AUTHORIZATION_COMPLETED transactions`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())
      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate = Instant.now().toString()
      val walletId = UUID.randomUUID().toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      (transactionAuthorizationRequestedEvent.data.transactionGatewayAuthorizationRequestedData
          as NpgTransactionGatewayAuthorizationRequestedData)
        .walletInfo = WalletInfo(walletId, null)
      transactionAuthorizationRequestedEvent.creationDate = authDate
      val transactionId = TransactionId(TRANSACTION_ID)
      val transactionAuthorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.DECLINED))
      val events: List<TransactionEvent<Any>> =
        listOf(
          transactionActivatedEvent as TransactionEvent<Any>,
          transactionAuthorizationRequestedEvent as TransactionEvent<Any>,
          transactionAuthorizationCompletedEvent as TransactionEvent<Any>)

      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      given(userStatsServiceClient.saveLastUsage(any(), any())).willReturn(Mono.empty())
      given(checkpointer.success()).willReturn(Mono.empty())

      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(0))
        .getStateNpg(any(), any(), any(), any(), any())
      val expectedUserLastPaymentMethodDataDto =
        WalletLastUsageData()
          .walletId(UUID.fromString(walletId))
          .date(OffsetDateTime.parse(authDate)) as UserLastPaymentMethodData
      verify(userStatsServiceClient, times(1))
        .saveLastUsage(UUID.fromString(USER_ID), expectedUserLastPaymentMethodDataDto)
      verify(transactionsServiceClient, times(0)).patchAuthRequest(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
    }

  @Test
  fun `messageReceiver consume postponed authorization requested event correctly performing NPG get state`() =
    runTest {
      val transactionActivatedEvent =
        transactionActivateEvent(npgTransactionGatewayActivationData())
      val transactionAuthorizationRequestedEvent =
        transactionAuthorizationRequestedEvent(
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          npgTransactionGatewayAuthorizationRequestedData())

      val paymentInstrumentId = UUID.randomUUID().toString()
      val authDate =
        OffsetDateTime.now()
          .minus(Duration.ofSeconds(firstAttemptOffsetSeconds.toLong()))
          .toString()
      transactionAuthorizationRequestedEvent.data.paymentInstrumentId = paymentInstrumentId
      transactionAuthorizationRequestedEvent.creationDate = authDate
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
              .operationTime("2024-01-01T00:00:00")
              .additionalData(mapOf("authorizationCode" to authorizationCode, "rrn" to rrn)))

      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            transactionId.value()))
        .willReturn(Flux.fromIterable(events))
      given(authorizationStateRetrieverService.getStateNpg(any(), any(), any(), any(), any()))
        .willReturn(mono { npgStateResponse })
      given(transactionsServiceClient.patchAuthRequest(any(), any()))
        .willReturn(
          mono {
            UpdateAuthorizationResponseDto().status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
          })
      given(authRequestedQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(checkpointer.success()).willReturn(Mono.empty())

      // Test
      StepVerifier.create(
          authorizationRequestedHelper.authorizationRequestedHandler(
            QueueEvent(transactionAuthorizationRequestedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            checkpointer))
        .expectNext(Unit)
        .verifyComplete()
      // assertions
      verify(authorizationStateRetrieverService, times(1))
        .getStateNpg(any(), any(), any(), any(), any())

      verify(userStatsServiceClient, times(0)).saveLastUsage(any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      verify(authRequestedQueueAsyncClient, times(0))
        .sendMessageWithResponse(any<BinaryData>(), any(), any())
    }
}
