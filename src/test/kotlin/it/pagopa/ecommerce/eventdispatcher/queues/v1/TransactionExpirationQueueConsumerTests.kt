package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TRANSIENT_QUEUE_TTL_SECONDS
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import java.time.Duration
import java.time.ZonedDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.messaging.MessageHeaders
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionExpirationQueueConsumerTests {

  private val checkpointer: Checkpointer = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionsExpiredEventStoreRepository:
    TransactionsEventStoreRepository<TransactionExpiredData> =
    mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val refundRetryService: RefundRetryService = mock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  @Captor
  private lateinit var transactionExpiredEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionExpiredData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  @Captor private lateinit var queueEventCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var binaryDataCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var visibilityTimeoutCaptor: ArgumentCaptor<Duration>

  private val transactionUtils = TransactionUtils()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val expirationQueueAsyncClient: QueueAsyncClient = mock()

  private val sendPaymentResultTimeout = 120

  private val sendPaymentResultOffset = 10

  private val tracingUtils = TracingUtilsTests.getMock()

  private val transactionExpirationQueueConsumer =
    TransactionExpirationQueueConsumer(
      paymentGatewayClient = paymentGatewayClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionsExpiredEventStoreRepository = transactionsExpiredEventStoreRepository,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      transactionUtils = transactionUtils,
      refundRetryService = refundRetryService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      expirationQueueAsyncClient = expirationQueueAsyncClient,
      sendPaymentResultTimeoutSeconds = sendPaymentResultTimeout,
      sendPaymentResultTimeoutOffsetSeconds = sendPaymentResultOffset,
      transientQueueTTLSeconds = TRANSIENT_QUEUE_TTL_SECONDS,
      tracingUtils = tracingUtils)

  @Test
  fun `messageReceiver receives activated messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId,
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives legacy activated messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId,
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to null,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives legacy expiration messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val tx = reduceEvents(activatedEvent)

    val expiredEvent = transactionExpiredEvent(tx)

    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId,
        ))
      .willReturn(
        Flux.just(activatedEvent as TransactionEvent<Any>, expiredEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.EXPIRED_NOT_AUTHORIZED, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.right<TransactionActivatedEvent, TransactionExpiredEvent>(expiredEvent) to null,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))

    val transaction =
      transactionDocument(
        TransactionStatusDto.EXPIRED, ZonedDateTime.parse(activatedEvent.creationDate))

    val gatewayClientResponse = VposDeleteResponseDto()
    gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
  }

  @Test
  fun `messageReceiver generate new expired event with error in eventstore`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.empty())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
  }

  @Test
  fun `messageReceiver fails to generate new expired event`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))

    val gatewayClientResponse = VposDeleteResponseDto()
    gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CREATED)

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

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.findByTransactionId(any()))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))
    given(transactionsViewRepository.save(any()))
      .willReturn(Mono.error(RuntimeException("error while trying to save event")))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
  }

  @Test
  fun `messageReceiver fails to generate new refund event`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))

    val gatewayClientResponse = VposDeleteResponseDto()
    gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CREATED)

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

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.findByTransactionId(any()))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))
    given(transactionsViewRepository.save(any()))
      .willReturn(Mono.error(RuntimeException("error while saving data")))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
  }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request and PGS response KO generating refunded event`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      EmptyTransaction().applyEvent(activatedEvent).applyEvent(authorizationRequestedEvent)

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.AUTHORIZED)

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

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture(), any()))
        .willReturn(Mono.empty())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.AUTHORIZATION_COMPLETED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(3)).save(any())
      assertEquals(0, retryCountCaptor.value)
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT)
      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUND_ERROR)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request and PGS response OK generating refund error event`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.AUTHORIZATION_COMPLETED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(3)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUNDED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver calls update transaction to EXPIRED_NOT_AUTHORIZED for activated only expired transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      assertEquals(
        TransactionStatusDto.EXPIRED_NOT_AUTHORIZED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver does nothing on a expiration event received for a transaction in EXPIRED_NOT_AUTHORIZED status`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val transactionExpiredEvent = transactionExpiredEvent(reduceEvents(activatedEvent))

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            transactionExpiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request after transaction expiration`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val expiredEvent =
        transactionExpiredEvent(reduceEvents(activatedEvent, authorizationRequestedEvent))
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(2)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      val viewExpectedStatuses =
        listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver calls refund on transaction expired in NOTIFIED_KO status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val userReceiptRequestedEvent = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val addUserReceiptEvent = transactionUserReceiptAddedEvent(transactionUserReceiptData)
    val expiredEvent =
      transactionExpiredEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          addUserReceiptEvent))

    val gatewayClientResponse = VposDeleteResponseDto()
    gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
          authorizationCompletedEvent as TransactionEvent<Any>,
          closedEvent as TransactionEvent<Any>,
          addUserReceiptEvent as TransactionEvent<Any>,
          userReceiptRequestedEvent as TransactionEvent<Any>,
          expiredEvent as TransactionEvent<Any>,
        ))

    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
          Mono.just(
            transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
    verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
    verify(transactionsViewRepository, times(2)).save(any())
    /*
     * check view update statuses and events stored into event store
     */
    val expectedRefundEventStatuses =
      listOf(
        TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
    val viewExpectedStatuses =
      listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
    viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
      assertEquals(
        expectedStatus,
        transactionViewRepositoryCaptor.allValues[idx].status,
        "Unexpected view status on idx: $idx")
    }

    expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
      assertEquals(
        expectedStatus,
        TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
        "Unexpected event code on idx: $idx")
    }
  }

  @Test
  fun `messageReceiver calls refund on transaction in NOTIFICATION_ERROR status and send payment result outcome KO`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(3)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUNDED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not calls refund on transaction in NOTIFICATION_ERROR status and send payment result outcome OK`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      assertEquals(TransactionStatusDto.EXPIRED, transactionViewRepositoryCaptor.value.status)
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver calls refund on transaction expired in NOTIFICATION_ERROR status and send payment result outcome KO`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closedEvent,
            userReceiptRequestedEvent,
            userReceiptErrorEvent))
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(2)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      val viewExpectedStatuses =
        listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver should not calls refund on transaction expired in NOTIFICATION_ERROR status and send payment result outcome OK`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closedEvent,
            userReceiptRequestedEvent,
            userReceiptErrorEvent,
          ))
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should not process transaction in REFUND_REQUESTED status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val userReceiptRequestedEvent = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
    val refundRequestedEvent =
      transactionRefundRequestedEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          userReceiptErrorEvent))
    val gatewayClientResponse = VposDeleteResponseDto()
    gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
          authorizationCompletedEvent as TransactionEvent<Any>,
          closedEvent as TransactionEvent<Any>,
          userReceiptRequestedEvent as TransactionEvent<Any>,
          userReceiptErrorEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>))

    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0)).save(any())
  }

  @Test
  fun `messageReceiver should not process transaction in REFUND_ERROR status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val userReceiptRequestedEvent = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
    val refundRequestedEvent =
      transactionRefundRequestedEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          userReceiptErrorEvent))
    val refundErrorEvent =
      transactionRefundErrorEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          userReceiptErrorEvent,
          refundRequestedEvent))
    val gatewayClientResponse = VposDeleteResponseDto()
    gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
          authorizationCompletedEvent as TransactionEvent<Any>,
          closedEvent as TransactionEvent<Any>,
          userReceiptRequestedEvent as TransactionEvent<Any>,
          userReceiptErrorEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>,
          refundErrorEvent as TransactionEvent<Any>))

    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
            MOCK_TRACING_INFO,
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
    verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0)).save(any())
  }

  @Test
  fun `messageReceiver calls update transaction to CANCELLATION_EXPIRED for transaction expired in CANCELLATION_REQUESTED status`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val cancellationRequested = transactionUserCanceledEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            cancellationRequested as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      assertEquals(
        TransactionStatusDto.CANCELLATION_EXPIRED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver calls update transaction to CANCELLATION_EXPIRED for transaction expired in CLOSURE_ERROR coming from user cancellation`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val cancellationRequested = transactionUserCanceledEvent()
      val closureError = transactionClosureErrorEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            cancellationRequested as TransactionEvent<Any>,
            closureError as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      assertEquals(
        TransactionStatusDto.CANCELLATION_EXPIRED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver does nothing on a expiration event received for a transaction in CANCELLATION_EXPIRED status`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val cancellationEvent = transactionUserCanceledEvent()
      val transactionExpiredEvent = transactionExpiredEvent(reduceEvents(activatedEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            cancellationEvent as TransactionEvent<Any>,
            transactionExpiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should not calls refund on transaction in CLOSURE_ERROR status for an authorized transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closureErrorEvent = transactionClosureErrorEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
        )
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
    }

  @Test
  fun `messageReceiver should not call refund on transaction in CLOSURE_ERROR status for an authorized transaction that has expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closureErrorEvent = transactionClosureErrorEvent()
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureErrorEvent))

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())

      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
    }

  @Test
  fun `messageReceiver should not calls refund on transaction in CLOSURE_ERROR status for an user canceled transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val userCanceledEvent = transactionUserCanceledEvent()
      val closureErrorEvent = transactionClosureErrorEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            userCanceledEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val viewExpectedStatuses = listOf(TransactionStatusDto.CANCELLATION_EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not call refund on transaction in CLOSURE_ERROR status for an unauthorized transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO)
      val closureErrorEvent = transactionClosureErrorEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should do nothing on transaction in CLOSURE_ERROR status for an unauthorized transaction that has expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO)
      val closureErrorEvent = transactionClosureErrorEvent()
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureErrorEvent))

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in AUTHORIZATION_COMPLETED status that with outcome KO`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO)

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
            authorizationCompletedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now()))))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.AUTHORIZATION_COMPLETED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in AUTHORIZATION_COMPLETED status that with outcome KO expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
          ))

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver forward event into dead letter queue for exception processing the event`() =
    runTest {
      /* preconditions */

      val activatedEvent = transactionActivateEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(Flux.error(RuntimeException("Error finding event from event store")))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    }

  @Test
  fun `messageReceiver processing should fail for error forward event into dead letter queue for exception processing the event`() =
    runTest {
      /* preconditions */

      val activatedEvent = transactionActivateEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(Flux.error(RuntimeException("Error finding event from event store")))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(Mono.error(RuntimeException("Error sending event to dead letter queue")))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectErrorMatches { it.message == "Error sending event to dead letter queue" }
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in AUTHORIZATION_COMPLETED status with auth outcome OK`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.AUTHORIZATION_COMPLETED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
      /*
       * check view update statuses and events stored into event store
       */
      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.AUTHORIZATION_COMPLETED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in AUTHORIZATION_COMPLETED status with auth outcome OK expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(activatedEvent, authorizationRequestedEvent, authorizationCompletedEvent))
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.AUTHORIZATION_COMPLETED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should perform refund for a transaction in CLOSED status with close payment response outcome KO`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.KO)
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(3)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
          TransactionStatusDto.REFUND_REQUESTED,
          TransactionStatusDto.REFUNDED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should perform refund for a transaction in CLOSED status with close payment response outcome KO expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.KO)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent, authorizationRequestedEvent, authorizationCompletedEvent, closedEvent))
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(paymentGatewayClient.requestVPosRefund(any()))
        .willReturn(Mono.just(gatewayClientResponse))

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(2)).save(any())
      verify(transactionsViewRepository, times(2)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(
          TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
          TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
      val viewExpectedStatuses =
        listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in CLOSED status with close payment response outcome OK`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      closedEvent.creationDate =
        ZonedDateTime.now().minus(Duration.ofSeconds(sendPaymentResultTimeout.toLong())).toString()
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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory
                  .SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT)))
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in CLOSED status with close payment response outcome OK expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent, authorizationRequestedEvent, authorizationCompletedEvent, closedEvent))

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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should enqueue expiration event to wait for send payment result to be received`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closePaymentDate = ZonedDateTime.now()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      closedEvent.creationDate = closePaymentDate.toString()
      val event =
        Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
          MOCK_TRACING_INFO
      val expectedRetryEventVisibilityTimeout =
        Duration.ofSeconds(sendPaymentResultTimeout.toLong())
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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          expirationQueueAsyncClient.sendMessageWithResponse(
            queueEventCaptor.capture(), visibilityTimeoutCaptor.capture(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            event, checkpointer, MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(expirationQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          argThat<Duration> {
            expectedRetryEventVisibilityTimeout.toSeconds() - this.toSeconds() <= 1
          },
          eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in CLOSED status with closePayment authorization outcome OK near sendPaymentResult expiration offset`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      closedEvent.creationDate =
        ZonedDateTime.now()
          .minus(Duration.ofSeconds(sendPaymentResultTimeout.toLong()))
          .plus(Duration.ofSeconds(sendPaymentResultOffset.toLong() / 2))
          .toString()
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
            authorizationCompletedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(activatedEvent) to
              MOCK_TRACING_INFO,
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {})
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory
                  .SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT)))
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSED, expiredEvent.data.statusBeforeExpiration)
    }
}
