package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.storage.queue.QueueAsyncClient
import com.azure.storage.queue.models.SendMessageResult
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TooLateRetryAttemptException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.utils.TRANSIENT_QUEUE_TTL_SECONDS
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionsViewProjectionHandler
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.util.stream.Stream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.core.env.Environment
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class AuthorizationStateRetrieverRetryServiceTest {

  private val paymentTokenValidityTimeOffset = 10

  private val authRequestedOutcomeWaitingQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val eventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRetriedData> =
    mock()
  private val retryOffset: Int = 10
  private val maxAttempts: Int = 3

  private val strictSerializerProviderV2: StrictJsonSerializerProvider =
    QueuesConsumerConfig().strictSerializerProviderV2()

  @Captor
  private lateinit var eventStoreCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRetriedData>>

  @Captor private lateinit var viewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor private lateinit var queueCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var durationCaptor: ArgumentCaptor<Duration>

  private val authorizationStateRetrieverRetryService =
    AuthorizationStateRetrieverRetryService(
      paymentTokenValidityTimeOffset = paymentTokenValidityTimeOffset,
      authRequestedOutcomeWaitingQueueAsyncClient = authRequestedOutcomeWaitingQueueAsyncClient,
      viewRepository = transactionsViewRepository,
      eventStoreRepository = eventStoreRepository,
      retryOffset = retryOffset,
      maxAttempts = maxAttempts,
      transientQueuesTTLSeconds = TRANSIENT_QUEUE_TTL_SECONDS,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  var mockedEnv: Environment = mock<Environment>() as Environment

  val ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG = "transactionsview.update.enabled"

  @BeforeEach
  fun setUp() {
    TransactionsViewProjectionHandler.env = mockedEnv
  }

  @Test
  fun `Should enqueue new authorization requested retry event for left remaining attempts`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), eq(Duration.ofSeconds((retryOffset * 3).toLong())), anyOrNull()))
      .willReturn(queueSuccessfulResponse())

    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, maxAttempts - 1, TracingInfoTest.MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val savedView = viewRepositoryCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(TransactionStatusDto.AUTHORIZATION_REQUESTED, savedView.status)
    assertEquals(
      maxAttempts,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
          strictSerializerProviderV2.createInstance())
        .event
        .data
        .retryCount)
  }

  @Test
  fun `Should enqueue new authorization requested retry event for left remaining attempts with no transactions-view update if ff disabled`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), eq(Duration.ofSeconds((retryOffset * 3).toLong())), anyOrNull()))
      .willReturn(queueSuccessfulResponse())

    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, maxAttempts - 1, TracingInfoTest.MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(
      maxAttempts,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
          strictSerializerProviderV2.createInstance())
        .event
        .data
        .retryCount)
  }

  @Test
  fun `Should enqueue new authorization requested retry event for first time sending event`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, 0, TracingInfoTest.MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val savedView = viewRepositoryCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(TransactionStatusDto.AUTHORIZATION_REQUESTED, savedView.status)
    assertEquals(
      1,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
          strictSerializerProviderV2.createInstance())
        .event
        .data
        .retryCount)
    assertEquals(retryOffset, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should enqueue new authorization requested retry event for first time sending event with no transactions-view update if ff disabled`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, 0, TracingInfoTest.MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(
      1,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
          strictSerializerProviderV2.createInstance())
        .event
        .data
        .retryCount)
    assertEquals(retryOffset, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should not enqueue new authorization requested retry event for no left remaining attempts`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationOutcomeWaitingEvent(maxAttempts)
          as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, maxAttempts, TracingInfoTest.MOCK_TRACING_INFO))
      .expectError(NoRetryAttemptsLeftException::class.java)
      .verify()

    verify(eventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should not enqueue new authorization requested retry event for no time remaining until payment token expires`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent(
          Instant.ofEpochMilli(0L).toString(),
          TransactionTestUtils.npgTransactionGatewayActivationData()) as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())

    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, 0, TracingInfoTest.MOCK_TRACING_INFO))
      .expectError(TooLateRetryAttemptException::class.java)
      .verify()

    verify(eventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should not enqueue new authorization requested retry event for error saving event to eventstore`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.error<TransactionEvent<Any>>(RuntimeException("Error saving event into event store"))
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, maxAttempts - 1, TracingInfoTest.MOCK_TRACING_INFO))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  companion object {
    @JvmStatic
    fun `Visibility timeout validation test method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          ZonedDateTime.now().minus(Duration.ofMinutes(10)).toString(),
          Duration.ofMinutes(9),
          false),
        Arguments.of(
          ZonedDateTime.now().plus(Duration.ofMinutes(10)).toString(), Duration.ofMinutes(9), true),
      )
  }

  @ParameterizedTest
  @MethodSource("Visibility timeout validation test method source")
  fun `Should validate visibility timeout correctly`(
    creationDate: String,
    visibilityTimeout: Duration,
    expectedResult: Boolean
  ) {
    // pre-requisites
    val transactionActivatedEvent =
      TransactionTestUtils.transactionActivateEvent(
        creationDate, TransactionTestUtils.npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      TransactionTestUtils.transactionAuthorizationRequestedEvent()
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    // test
    val visibilityTimeoutCheck =
      authorizationStateRetrieverRetryService.validateRetryEventVisibilityTimeout(
        baseTransaction = baseTransaction, visibilityTimeout)
    assertEquals(expectedResult, visibilityTimeoutCheck)
  }

  @Test
  fun `Should not enqueue new authorization requested retry event for error retrieving transaction view`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())

    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.error<Transaction>(RuntimeException("Error finding transaction by id")) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, maxAttempts - 1, TracingInfoTest.MOCK_TRACING_INFO))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  @Test
  fun `Should not enqueue new authorization requested retry event for error updating transaction view`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.AUTHORIZATION_REQUESTED, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.error<Transaction>(RuntimeException("Error updating transaction view"))
    }
    given(
        authRequestedOutcomeWaitingQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        authorizationStateRetrieverRetryService.enqueueRetryEvent(
          baseTransaction, maxAttempts - 1, TracingInfoTest.MOCK_TRACING_INFO))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(authRequestedOutcomeWaitingQueueAsyncClient, times(0))
      .sendMessageWithResponse(any<BinaryData>(), any(), any())
  }

  private fun queueSuccessfulResponse(): Mono<Response<SendMessageResult>> {
    val sendMessageResult = SendMessageResult()
    sendMessageResult.messageId = "msgId"
    sendMessageResult.timeNextVisible = OffsetDateTime.now()
    return Mono.just(ResponseBase(null, 200, null, sendMessageResult, null))
  }
}
