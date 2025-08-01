package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.storage.queue.QueueAsyncClient
import com.azure.storage.queue.models.SendMessageResult
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRetriedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.activation.EmptyTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TooLateRetryAttemptException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.utils.TRANSIENT_QUEUE_TTL_SECONDS
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionsViewProjectionHandler
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.core.env.Environment
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class ClosureRetryServiceTests {

  private val closureRetryQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val eventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRetriedData> =
    mock()

  @Captor
  private lateinit var eventStoreCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRetriedData>>

  @Captor private lateinit var viewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor private lateinit var queueCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var durationCaptor: ArgumentCaptor<Duration>

  private val maxAttempts = 3

  private val closureRetryOffset = 10

  private val paymentTokenValidityTimeOffset = 10

  private val jsonSerializerV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val closureRetryService =
    ClosureRetryService(
      closureRetryQueueAsyncClient = closureRetryQueueAsyncClient,
      closePaymentRetryOffset = closureRetryOffset,
      maxAttempts = maxAttempts,
      viewRepository = transactionsViewRepository,
      eventStoreRepository = eventStoreRepository,
      paymentTokenValidityTimeOffset = paymentTokenValidityTimeOffset,
      transientQueuesTTLSeconds = TRANSIENT_QUEUE_TTL_SECONDS,
      strictSerializerProviderV2 = jsonSerializerV2)

  var mockedEnv: Environment = mock<Environment>() as Environment

  val ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG = "transactionsview.update.enabled"

  @BeforeEach
  fun setUp() {
    TransactionsViewProjectionHandler.env = mockedEnv
  }

  @Test
  fun `Should enqueue new closure retry event for left remaining attempts`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1, MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(closureRetryQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val savedView = viewRepositoryCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, savedView.status)
    assertEquals(
      maxAttempts,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionClosureRetriedEvent>>() {},
          jsonSerializerV2.createInstance())
        .event
        .data
        .retryCount)
    assertEquals(closureRetryOffset * 3, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should enqueue new closure retry event for first time sending event`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, 0, MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(closureRetryQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val savedView = viewRepositoryCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, savedView.status)
    assertEquals(
      1,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionClosureRetriedEvent>>() {},
          jsonSerializerV2.createInstance())
        .event
        .data
        .retryCount)
    assertEquals(closureRetryOffset, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should enqueue new closure retry event for first time sending event with no transactions-view update if ff disabled`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("false")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, 0, MOCK_TRACING_INFO))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    val savedEvent = eventStoreCaptor.value
    val eventSentOnQueue = queueCaptor.value
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT,
      TransactionEventCode.valueOf(savedEvent.eventCode))
    assertEquals(
      1,
      eventSentOnQueue
        .toObject(
          object : TypeReference<QueueEvent<TransactionClosureRetriedEvent>>() {},
          jsonSerializerV2.createInstance())
        .event
        .data
        .retryCount)
    assertEquals(closureRetryOffset, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should not enqueue new closure retry event for no left remaining attempts`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts, MOCK_TRACING_INFO))
      .expectError(NoRetryAttemptsLeftException::class.java)
      .verify()

    verify(eventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for error saving event to eventstore`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.error<TransactionEvent<Any>>(RuntimeException("Error saving event into event store"))
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1, MOCK_TRACING_INFO))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for error retrieving transaction view`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

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
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1, MOCK_TRACING_INFO))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for error updating transaction view`() {
    whenever(mockedEnv.getProperty(ENV_TRANSACTIONS_VIEW_UPDATED_ENABLED_FLAG, "true"))
      .thenReturn("true")
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.error<Transaction>(RuntimeException("Error updating transaction view"))
    }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1, MOCK_TRACING_INFO))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for retry event with visibility timeout over transaction payment token validity`() {
    val creationDate =
      ZonedDateTime.now()
        .minus(Duration.ofSeconds(TransactionTestUtils.PAYMENT_TOKEN_VALIDITY_TIME_SEC.toLong()))
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent(
          creationDate.toString(), EmptyTransactionGatewayActivationData())
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionClosureErrorEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    val transactionDocument =
      TransactionTestUtils.transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())
    given(eventStoreRepository.save(eventStoreCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willAnswer { Mono.just(transactionDocument) }
    given(transactionsViewRepository.save(viewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(
        closureRetryQueueAsyncClient.sendMessageWithResponse(
          queueCaptor.capture(), durationCaptor.capture(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())
    StepVerifier.create(
        closureRetryService.enqueueRetryEvent(baseTransaction, 0, MOCK_TRACING_INFO))
      .expectError(TooLateRetryAttemptException::class.java)
      .verify()

    verify(eventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
  }

  private fun queueSuccessfulResponse(): Mono<Response<SendMessageResult>> {
    val sendMessageResult = SendMessageResult()
    sendMessageResult.messageId = "msgId"
    sendMessageResult.timeNextVisible = OffsetDateTime.now()
    return Mono.just(ResponseBase(null, 200, null, sendMessageResult, null))
  }
}
