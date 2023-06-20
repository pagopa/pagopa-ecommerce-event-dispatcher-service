package it.pagopa.ecommerce.eventdispatcher.services.eventretry

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.core.util.BinaryData
import com.azure.storage.queue.QueueAsyncClient
import com.azure.storage.queue.models.SendMessageResult
import it.pagopa.ecommerce.commons.documents.v1.Transaction
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TooLateRetryAttemptException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.utils.TRANSIENT_QUEUE_TTL_MINUTES
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class ClosureRetryServiceTests {

  private val closureRetryQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData> =
    mock()

  @Captor
  private lateinit var eventStoreCaptor: ArgumentCaptor<TransactionEvent<TransactionRetriedData>>

  @Captor private lateinit var viewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor private lateinit var queueCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var durationCaptor: ArgumentCaptor<Duration>

  private val maxAttempts = 3

  private val closureRetryOffset = 10

  private val paymentTokenValidityTimeOffset = 10

  private val closureRetryService =
    ClosureRetryService(
      closureRetryQueueAsyncClient = closureRetryQueueAsyncClient,
      closePaymentRetryOffset = closureRetryOffset,
      maxAttempts = maxAttempts,
      viewRepository = transactionsViewRepository,
      eventStoreRepository = eventStoreRepository,
      paymentTokenValidityTimeOffset = paymentTokenValidityTimeOffset,
      transientQueuesTTLSeconds = TRANSIENT_QUEUE_TTL_MINUTES)

  @Test
  fun `Should enqueue new closure retry event for left remaining attempts`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(closureRetryQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
    val savedEvent = eventStoreCaptor.value
    val savedView = viewRepositoryCaptor.value
    val eventSentOnQueue = queueCaptor.value.toObject(TransactionRefundRetriedEvent::class.java)
    assertEquals(TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT, savedEvent.eventCode)
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, savedView.status)
    assertEquals(maxAttempts, eventSentOnQueue.data.retryCount)
    assertEquals(closureRetryOffset * 3, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should enqueue new closure retry event for first time sending event`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, 0))
      .expectNext()
      .verifyComplete()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(closureRetryQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
    val savedEvent = eventStoreCaptor.value
    val savedView = viewRepositoryCaptor.value
    val eventSentOnQueue = queueCaptor.value.toObject(TransactionRefundRetriedEvent::class.java)
    assertEquals(TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT, savedEvent.eventCode)
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, savedView.status)
    assertEquals(1, eventSentOnQueue.data.retryCount)
    assertEquals(closureRetryOffset, durationCaptor.value.seconds.toInt())
  }

  @Test
  fun `Should not enqueue new closure retry event for no left remaining attempts`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts))
      .expectError(NoRetryAttemptsLeftException::class.java)
      .verify()

    verify(eventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for error saving event to eventstore`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for error retrieving transaction view`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for error updating transaction view`() {
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, maxAttempts - 1))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()

    verify(eventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(1)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
  }

  @Test
  fun `Should not enqueue new closure retry event for retry event with visibility timeout over transaction payment token validity`() {
    val creationDate =
      ZonedDateTime.now()
        .minus(Duration.ofSeconds(TransactionTestUtils.PAYMENT_TOKEN_VALIDITY_TIME_SEC.toLong()))
    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(
        TransactionTestUtils.transactionActivateEvent(creationDate.toString())
          as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>,
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
    StepVerifier.create(closureRetryService.enqueueRetryEvent(baseTransaction, 0))
      .expectError(TooLateRetryAttemptException::class.java)
      .verify()

    verify(eventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0))
      .findByTransactionId(TransactionTestUtils.TRANSACTION_ID)
    verify(transactionsViewRepository, times(0)).save(any())
    verify(closureRetryQueueAsyncClient, times(0))
      .sendMessageWithResponse(
        any<BinaryData>(), any(), eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_MINUTES.toLong())))
  }

  private fun queueSuccessfulResponse(): Mono<Response<SendMessageResult>> {
    val sendMessageResult = SendMessageResult()
    sendMessageResult.messageId = "msgId"
    sendMessageResult.timeNextVisible = OffsetDateTime.now()
    return Mono.just(ResponseBase(null, 200, null, sendMessageResult, null))
  }
}
