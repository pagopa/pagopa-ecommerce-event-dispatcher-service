package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.redis.templatewrappers.PaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v1.NodeService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
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
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.http.HttpStatus
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionClosePaymentQueueConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val nodeService: NodeService = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val transactionClosureErrorEventStoreRepository: TransactionsEventStoreRepository<Void> =
    mock()

  private val closureRetryService: ClosureRetryService = mock()

  private val transactionClosedEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData> =
    mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val paymentRequestInfoRedisTemplateWrapper: PaymentRequestInfoRedisTemplateWrapper =
    mock()

  private val tracingUtils: TracingUtils = TracingUtilsTests.getMock()

  @Captor private lateinit var viewArgumentCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var closedEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<TransactionClosureData>>

  @Captor
  private lateinit var closureErrorEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionClosureErrorEvent>

  private val transactionClosureEventsConsumer =
    TransactionClosePaymentQueueConsumer(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionClosureSentEventRepository = transactionClosedEventRepository,
      transactionClosureErrorEventStoreRepository = transactionClosureErrorEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      nodeService = nodeService,
      closureRetryService = closureRetryService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      paymentRequestInfoRedisTemplateWrapper = paymentRequestInfoRedisTemplateWrapper,
    )

  @Test
  fun `consumer processes bare close message correctly with OK closure outcome`() = runTest {
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

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to MOCK_TRACING_INFO, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.OK,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
  }

  @Test
  fun `consumer processes bare legacy close message correctly with OK closure outcome`() = runTest {
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

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to null, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.OK,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
  }

  @Test
  fun `consumer processes bare close message correctly with KO closure outcome`() = runTest {
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
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to MOCK_TRACING_INFO, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), any())

    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
  }

  @Test
  fun `consumer processes bare legacy close message correctly with KO closure outcome`() = runTest {
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
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to null, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(transactionId, ClosePaymentOutcome.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      TransactionEventCode.valueOf(closedEventStoreRepositoryCaptor.value.eventCode))
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
  }

  @Test
  fun `consumer receive error from close payment and send a retry event`() = runTest {
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

    given(closureRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to MOCK_TRACING_INFO, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
    // mocking
    verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
      TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
  }

  @Test
  fun `consumer process doesn't modify db on invalid transaction status`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))
    val payload = BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO))
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
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to null, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(0)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).never()).deleteById(any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(object : TypeReference<TransactionUserCanceledEvent>() {}).eventCode) ==
            TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
  }

  @Test
  fun `consumer receive unrecoverable error (400 Bad Request) error from close payment and do not send a retry event`() =
    runTest {
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

      given(closureRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
      /* test */
      Hooks.onOperatorDebug()
      StepVerifier.create(
          transactionClosureEventsConsumer.messageReceiver(
            cancelRequestEvent as TransactionUserCanceledEvent to MOCK_TRACING_INFO, checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), eq(MOCK_TRACING_INFO))
    }

  @Test
  fun `consumer receive unrecoverable error (404 Not found) error from close payment and do not send a retry event`() =
    runTest {
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
        .willThrow(ClosePaymentErrorResponseException(HttpStatus.NOT_FOUND, ErrorDto()))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(closureRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          transactionClosureEventsConsumer.messageReceiver(
            cancelRequestEvent as TransactionUserCanceledEvent to MOCK_TRACING_INFO, checkpointer))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any(), eq(MOCK_TRACING_INFO))
    }

  companion object {

    @JvmStatic
    fun nodeErrorResponsesForEnqueueRetryTest(): Stream<Throwable> =
      Stream.of(
        ClosePaymentErrorResponseException(
          statusCode = HttpStatus.INTERNAL_SERVER_ERROR,
          errorResponse = ErrorDto().outcome("KO").description("Internal Server error")),
        ClosePaymentErrorResponseException(statusCode = null, errorResponse = null))
  }

  @ParameterizedTest
  @MethodSource("nodeErrorResponsesForEnqueueRetryTest")
  fun `consumer receive handle error from Node during close payment enqueueing retry event`(
    throwable: Throwable
  ) = runTest {
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
    given(nodeService.closePayment(transactionId, ClosePaymentOutcome.KO)).willThrow(throwable)

    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }

    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(closureRetryService.enqueueRetryEvent(any(), any(), any())).willReturn(Mono.empty())
    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          cancelRequestEvent as TransactionUserCanceledEvent to MOCK_TRACING_INFO, checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(paymentRequestInfoRedisTemplateWrapper, Mockito.after(1000).times(1)).deleteById(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any(), any())
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
      TransactionEventCode.valueOf(closureErrorEventStoreRepositoryCaptor.value.eventCode))
  }
}
