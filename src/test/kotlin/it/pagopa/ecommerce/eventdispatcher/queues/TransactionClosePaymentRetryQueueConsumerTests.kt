package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.NodeService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DEAD_LETTER_QUEUE_TTL_SECONDS
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionClosePaymentRetryQueueConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val nodeService: NodeService = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val closureRetryService: ClosureRetryService = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()
  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionClosedEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData> =
    mock()

  private val refundRetryService: RefundRetryService = mock()

  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  @Captor private lateinit var viewArgumentCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var refundedEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  @Captor
  private lateinit var closedEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<TransactionClosureData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val transactionClosureErrorEventsConsumer =
    TransactionClosePaymentRetryQueueConsumer(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionClosureSentEventRepository = transactionClosedEventRepository,
      transactionsViewRepository = transactionsViewRepository,
      nodeService = nodeService,
      closureRetryService = closureRetryService,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      paymentGatewayClient = paymentGatewayClient,
      refundRetryService = refundRetryService,
      deadLetterQueueAsyncClient = deadLetterQueueAsyncClient,
      deadLetterTTLSeconds = DEAD_LETTER_QUEUE_TTL_SECONDS,
      tracingUtils = tracingUtils)

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for authorization completed transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.CLOSED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer processes bare closure error message correctly with KO closure outcome for unauthorized transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.UNAUTHORIZED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer processes bare legacy closure error message correctly with OK closure outcome for authorization completed transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.CLOSED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer processes bare legacy closure error message correctly with KO closure outcome for unauthorized transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(closureErrorEvent).toBytes(), checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.UNAUTHORIZED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for unauthorized transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.UNAUTHORIZED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.UNAUTHORIZED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSURE_FAILED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer processes bare closure error message correctly with KO closure outcome for user canceled transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val userCanceledEvent = transactionUserCanceledEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, userCanceledEvent, closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer processes bare closure error message correctly with OK closure outcome for user canceled transaction`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val userCanceledEvent = transactionUserCanceledEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, userCanceledEvent, closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(1))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.OK,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    }

  @Test
  fun `consumer error processing bare closure error message for authorized transaction with missing authorizationResultDto value`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(null) as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent,
          authorizationRequestEvent,
          authorizationCompletedEvent,
          closureErrorEvent)

      val expectedUpdatedTransaction =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      val expectedClosureEvent =
        TransactionClosureFailedEvent(
          activationEvent.transactionId, TransactionClosureData(TransactionClosureData.Outcome.OK))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionClosedEventRepository.save(any()))
        .willReturn(Mono.just(expectedClosureEvent))
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
      given(
          deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())
      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransaction)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(deadLetterQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            this.toObject(object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {})
              .event
              .eventCode == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
    }

  @Test
  fun `consumer error processing bare closure error message for closure error aggregate with unexpected transactionAtPreviousStep`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val emptyTransactionMock: EmptyTransaction = mock()
      val transactionWithClosureError: TransactionWithClosureError = mock()
      val fakeTransactionAtPreviousState = transactionActivated(ZonedDateTime.now().toString())

      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events = listOf(activatedEvent as TransactionEvent<Any>)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(emptyTransactionMock.applyEvent(any())).willReturn(transactionWithClosureError)
      given(transactionWithClosureError.transactionId).willReturn(TransactionId(TRANSACTION_ID))
      given(transactionWithClosureError.status).willReturn(TransactionStatusDto.CLOSURE_ERROR)
      given(transactionWithClosureError.transactionAtPreviousState)
        .willReturn(fakeTransactionAtPreviousState)
      given(
          deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())
      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer,
            emptyTransactionMock))
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(0))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(deadLetterQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            this.toObject(object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {})
              .event
              .eventCode == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
    }

  @Test
  fun `consumer processes closure retry message correctly`() = runTest {
    val closureRetriedEvent = transactionClosureRetriedEvent(0)

    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK) as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent, authorizationRequestEvent, authorizationUpdateEvent, closureErrorEvent)

    val expectedUpdatedTransaction =
      transactionDocument(
        TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    val expectedClosureEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionClosedEventRepository.save(any())).willReturn(Mono.just(expectedClosureEvent))
    given(
        nodeService.closePayment(
          TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    /* test */

    StepVerifier.create(
        transactionClosureErrorEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(closureRetriedEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
  }

  @Test
  fun `consumer processes legacy closure retry message correctly`() = runTest {
    val closureRetriedEvent = transactionClosureRetriedEvent(0)

    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK) as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationUpdateEvent,
        closureErrorEvent,
        closureRetriedEvent as TransactionEvent<Any>)

    val expectedUpdatedTransaction =
      transactionDocument(
        TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    val expectedClosureEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionClosedEventRepository.save(any())).willReturn(Mono.just(expectedClosureEvent))
    given(
        nodeService.closePayment(
          TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    /* test */

    StepVerifier.create(
        transactionClosureErrorEventsConsumer.messageReceiver(
          BinaryData.fromObject(closureRetriedEvent).toBytes(), checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
  }

  @Test
  fun `consumer process doesn't modify db on invalid transaction status`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationUpdateEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK) as TransactionEvent<Any>

    val events = listOf(activationEvent, authorizationRequestEvent, authorizationUpdateEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())

    /* test */

    StepVerifier.create(
        transactionClosureErrorEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(0)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> {
          this.toObject(object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {})
            .event
            .eventCode == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
        },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `consumer perform refund for authorized transaction and close payment response outcome KO`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSURE_ERROR,
                ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))),
            Mono.just(
              transactionDocument(
                TransactionStatusDto.REFUND_REQUESTED,
                ZonedDateTime.parse(activationEvent.creationDate)))))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(
          Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
        .willReturn(
          ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(2)).save(any())
      verify(transactionsViewRepository, Mockito.times(3)).save(any())
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())

      val expectedViewUpdateStatuses =
        listOf(
          TransactionStatusDto.CLOSED,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUNDED)
      val expectedEventsCodes =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
        assertEquals(
          transactionStatusDto,
          viewArgumentCaptor.allValues[idx].status,
          "Unexpected view status update at idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_CLOSED_EVENT,
        closedEventStoreRepositoryCaptor.value.eventCode)
      assertEquals(
        TransactionClosureData.Outcome.KO,
        closedEventStoreRepositoryCaptor.value.data.responseOutcome)
      expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
        assertEquals(
          transactionEventCode,
          refundedEventStoreRepositoryCaptor.allValues[idx].eventCode,
          "Unexpected event at idx: $idx")
      }
    }

  @Test
  fun `consumer enqueue retry event in case of error processing the received event`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willReturn {
      Mono.error(RuntimeException("Error updating view"))
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(
        Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
    given(
        nodeService.closePayment(
          TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
    given(closureRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
      .willReturn(Mono.empty())

    /* test */

    StepVerifier.create(
        transactionClosureErrorEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())

    verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any())

    val expectedViewUpdateStatuses =
      listOf(
        TransactionStatusDto.CLOSED,
      )

    expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
      assertEquals(
        transactionStatusDto,
        viewArgumentCaptor.allValues[idx].status,
        "Unexpected view status update at idx: $idx")
    }
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      closedEventStoreRepositoryCaptor.value.eventCode)
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    assertEquals(0, retryCountCaptor.value)
  }

  @Test
  fun `consumer enqueue retry event in case of error processing the input retry event`() = runTest {
    val retryCount = 1
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>
    val closureRetriedEvent = transactionClosureRetriedEvent(retryCount) as TransactionEvent<Any>
    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureErrorEvent,
        closureRetriedEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willReturn {
      Mono.error(RuntimeException("Error updating view"))
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(
        Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
    given(
        nodeService.closePayment(
          TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })
    given(closureRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
      .willReturn(Mono.empty())

    /* test */

    StepVerifier.create(
        transactionClosureErrorEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(closureRetriedEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionClosedEventRepository, Mockito.times(1)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(1)).save(any())

    verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any())

    val expectedViewUpdateStatuses =
      listOf(
        TransactionStatusDto.CLOSED,
      )

    expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
      assertEquals(
        transactionStatusDto,
        viewArgumentCaptor.allValues[idx].status,
        "Unexpected view status update at idx: $idx")
    }
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      closedEventStoreRepositoryCaptor.value.eventCode)
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
    assertEquals(1, retryCountCaptor.value)
  }

  @Test
  fun `consumer perform refund transaction with no left attempts `() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
    val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(
            transactionDocument(
              TransactionStatusDto.CLOSURE_ERROR,
              ZonedDateTime.parse(activationEvent.creationDate))),
          Mono.just(
            transactionDocument(
              TransactionStatusDto.REFUND_REQUESTED,
              ZonedDateTime.parse(activationEvent.creationDate)))))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(refundedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(
        Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
    given(
        nodeService.closePayment(
          TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
      .willThrow(RuntimeException("Nodo error"))

    given(closureRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
      .willReturn(
        Mono.error(
          NoRetryAttemptsLeftException(
            eventCode = TransactionEventCode.TRANSACTION_CLOSURE_RETRIED_EVENT,
            transactionId = TransactionId(UUID.randomUUID()))))
    /* test */

    StepVerifier.create(
        transactionClosureErrorEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(2)).save(any())
    verify(transactionsViewRepository, Mockito.times(2)).save(any())
    verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any())

    val expectedViewUpdateStatuses =
      listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
    val expectedEventsCodes =
      listOf(
        TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
    expectedViewUpdateStatuses.forEachIndexed { idx, transactionStatusDto ->
      assertEquals(
        transactionStatusDto,
        viewArgumentCaptor.allValues[idx].status,
        "Unexpected view status update at idx: $idx")
    }

    expectedEventsCodes.forEachIndexed { idx, transactionEventCode ->
      assertEquals(
        transactionEventCode,
        refundedEventStoreRepositoryCaptor.allValues[idx].eventCode,
        "Unexpected event at idx: $idx")
    }
  }

  @Test
  fun `consumer does not perform refund transaction for generic error in retry event enqueue`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

      val events =
        listOf(
          activationEvent, authorizationRequestEvent, authorizationCompleteEvent, closureErrorEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            refundedEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(
          Mono.just(VposDeleteResponseDto().status(VposDeleteResponseDto.StatusEnum.CANCELLED)))
      given(
          nodeService.closePayment(
            TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK))
        .willThrow(RuntimeException("Nodo error"))

      given(closureRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
        .willReturn(Mono.error(RuntimeException("Error enqueuing retry event")))

      given(
          deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())

      /* test */

      StepVerifier.create(
          transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(closureErrorEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1))
        .closePayment(TransactionId(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
      verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())
      verify(transactionsViewRepository, Mockito.times(0)).save(any())
      verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any())
      verify(deadLetterQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            this.toObject(object : TypeReference<QueueEvent<TransactionClosureErrorEvent>>() {})
              .event
              .eventCode == TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT
          },
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
    }
}
