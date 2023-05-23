package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import java.time.ZonedDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionExpirationQueueConsumerTests {

  @Mock private lateinit var checkpointer: Checkpointer

  @Mock private lateinit var transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>

  @Mock private lateinit var paymentGatewayClient: PaymentGatewayClient

  @Mock
  private lateinit var transactionsExpiredEventStoreRepository:
    TransactionsEventStoreRepository<TransactionExpiredData>

  @Mock
  private lateinit var transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>

  @Mock private lateinit var transactionsViewRepository: TransactionsViewRepository

  @Mock private lateinit var refundRetryService: RefundRetryService

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  @Captor
  private lateinit var transactionExpiredEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionExpiredData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  @Autowired private lateinit var transactionUtils: TransactionUtils

  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  @Test
  fun `messageReceiver receives activated messages successfully`() {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)

    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionId(
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
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives refund messages successfully`() {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)

    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val refundRetriedEvent = transactionRefundRetriedEvent(0)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionId(
          transactionId,
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request`() = runTest {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)

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
        transactionsEventStoreRepository.findByTransactionId(
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
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
  }

  @Test
  fun `messageReceiver generate new expired event with error in eventstore`() = runTest {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)

    val activatedEvent = transactionActivateEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionId(
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
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
  }

  @Test
  fun `messageReceiver fails to generate new expired event`() = runTest {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)

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
        transactionsEventStoreRepository.findByTransactionId(
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

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectError()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
  }

  @Test
  fun `messageReceiver fails to generate new refund event`() = runTest {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)

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
        transactionsEventStoreRepository.findByTransactionId(
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

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectError()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
  }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request and PGS response KO generating refunded event`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      EmptyTransaction().applyEvent(activatedEvent).applyEvent(authorizationRequestedEvent)

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CREATED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
      given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
        transactionExpiredEventStoreCaptor.value.eventCode)
      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request and PGS response OK generating refund error event`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
        transactionExpiredEventStoreCaptor.value.eventCode)
      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver calls update transaction to EXPIRED_NOT_AUTHORIZED for activated only expired transaction`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
        transactionExpiredEventStoreCaptor.value.eventCode)
      assertEquals(
        TransactionStatusDto.EXPIRED_NOT_AUTHORIZED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver does nothing on a expiration event received for a transaction in EXPIRED_NOT_AUTHORIZED status`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val transactionExpiredEvent = transactionExpiredEvent(reduceEvents(activatedEvent))

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val expiredEvent =
        transactionExpiredEvent(reduceEvents(activatedEvent, authorizationRequestedEvent))
      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver calls refund on transaction expired in NOTIFIED_KO status`() = runTest {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)
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
        transactionsEventStoreRepository.findByTransactionId(
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
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
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
        transactionRefundEventStoreCaptor.allValues[idx].eventCode,
        "Unexpected event code on idx: $idx")
    }
  }

  @Test
  fun `messageReceiver calls refund on transaction in NOTIFICATION_ERROR status and send payment result outcome KO`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)
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
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(TransactionEventCode.TRANSACTION_EXPIRED_EVENT, expiredEvent.eventCode)
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not calls refund on transaction in NOTIFICATION_ERROR status and send payment result outcome OK`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)
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
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
      assertEquals(TransactionEventCode.TRANSACTION_EXPIRED_EVENT, expiredEvent.eventCode)
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver calls refund on transaction expired in NOTIFICATION_ERROR status and send payment result outcome KO`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)
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
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver should not calls refund on transaction expired in NOTIFICATION_ERROR status and send payment result outcome OK`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)
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
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)
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
        transactionsEventStoreRepository.findByTransactionId(
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
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
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
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService,
        deadLetterQueueAsyncClient)
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
        transactionsEventStoreRepository.findByTransactionId(
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
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
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
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val cancellationRequested = transactionUserCanceledEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
        transactionExpiredEventStoreCaptor.value.eventCode)
      assertEquals(
        TransactionStatusDto.CANCELLATION_EXPIRED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver calls update transaction to CANCELLATION_EXPIRED for transaction expired in CLOSURE_ERROR coming from user cancellation`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val cancellationRequested = transactionUserCanceledEvent()
      val closureError = transactionClosureErrorEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
        transactionExpiredEventStoreCaptor.value.eventCode)
      assertEquals(
        TransactionStatusDto.CANCELLATION_EXPIRED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver does nothing on a expiration event received for a transaction in CANCELLATION_EXPIRED status`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val cancellationEvent = transactionUserCanceledEvent()
      val transactionExpiredEvent = transactionExpiredEvent(reduceEvents(activatedEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
  fun `messageReceiver calls refund on transaction in CLOSURE_ERROR status for an authorized transaction`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

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
          transactionsEventStoreRepository.findByTransactionId(
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
      given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
        .willReturn(Mono.empty())
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(TransactionEventCode.TRANSACTION_EXPIRED_EVENT, expiredEvent.eventCode)
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver calls refund on transaction in CLOSURE_ERROR status for an authorized transaction that has expired by batch`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

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
          transactionsEventStoreRepository.findByTransactionId(
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
      given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
        .willReturn(Mono.empty())
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
          transactionRefundEventStoreCaptor.allValues[idx].eventCode,
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver should not calls refund on transaction in CLOSURE_ERROR status for an user canceled transaction`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val userCanceledEvent = transactionUserCanceledEvent()
      val closureErrorEvent = transactionClosureErrorEvent()

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
      assertEquals(TransactionEventCode.TRANSACTION_EXPIRED_EVENT, expiredEvent.eventCode)
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not call refund on transaction in CLOSURE_ERROR status for an unauthorized transaction`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

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
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
      assertEquals(TransactionEventCode.TRANSACTION_EXPIRED_EVENT, expiredEvent.eventCode)
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should do nothing on transaction in CLOSURE_ERROR status for an unauthorized transaction that has expired by batch`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

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
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(AuthorizationResultDto.KO)

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
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
      assertEquals(TransactionEventCode.TRANSACTION_EXPIRED_EVENT, expiredEvent.eventCode)
      assertEquals(
        TransactionStatusDto.AUTHORIZATION_COMPLETED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not perform refund for a transaction in AUTHORIZATION_COMPLETED status that with outcome KO expired by batch`() =
    runTest {
      val transactionExpirationQueueConsumer =
        TransactionExpirationQueueConsumer(
          paymentGatewayClient,
          transactionsEventStoreRepository,
          transactionsExpiredEventStoreRepository,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          transactionUtils,
          refundRetryService,
          deadLetterQueueAsyncClient)

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

      val gatewayClientResponse = VposDeleteResponseDto()
      gatewayClientResponse.status(VposDeleteResponseDto.StatusEnum.CANCELLED)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionId(
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
            BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
        .expectNext()
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }
}
