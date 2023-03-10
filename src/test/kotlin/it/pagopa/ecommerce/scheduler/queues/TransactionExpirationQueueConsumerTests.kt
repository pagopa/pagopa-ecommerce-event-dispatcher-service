package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.ecommerce.scheduler.services.eventretry.RefundRetryService
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import java.time.ZonedDateTime
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionExpirationQueueConsumerTests {

  @Mock private lateinit var checkpointer: Checkpointer

  @Mock private lateinit var nodeService: NodeService

  @Mock private lateinit var refundService: RefundService

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
        refundRetryService)

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
        refundRetryService)

    val activatedEvent = TransactionTestUtils.transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(0)

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
        refundRetryService)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))

    val transactionId = activatedEvent.transactionId

    val transaction =
      Transaction(
        transactionId,
        activatedEvent.data.paymentNotices,
        activatedEvent.data.paymentNotices.sumOf { it.amount },
        activatedEvent.data.email,
        TransactionStatusDto.EXPIRED,
        activatedEvent.data.clientId,
        activatedEvent.creationDate)

    val gatewayClientResponse = PostePayRefundResponseDto()
    gatewayClientResponse.refundOutcome = "OK"

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
    given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestRefund(any())
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
        refundRetryService)

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

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(0)).requestRefund(any())
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
        refundRetryService)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))

    val transactionId = activatedEvent.transactionId

    val transaction =
      Transaction(
        transactionId,
        activatedEvent.data.paymentNotices,
        activatedEvent.data.paymentNotices.sumOf { it.amount },
        activatedEvent.data.email,
        TransactionStatusDto.EXPIRED,
        activatedEvent.data.clientId,
        activatedEvent.creationDate)

    val gatewayClientResponse = PostePayRefundResponseDto()
    gatewayClientResponse.refundOutcome = "KO"

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
        refundRetryService)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))

    val gatewayClientResponse = PostePayRefundResponseDto()
    gatewayClientResponse.refundOutcome = "KO"

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
          refundRetryService)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      EmptyTransaction().applyEvent(activatedEvent).applyEvent(authorizationRequestedEvent)
      val expiredEvent =
        transactionExpiredEvent(reduceEvents(activatedEvent, authorizationRequestedEvent))
      val refundRequestedEvent =
        transactionRefundRequestedEvent(
          reduceEvents(activatedEvent, authorizationRequestedEvent, expiredEvent))
      val refundedEvent =
        transactionRefundedEvent(
          reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))

      val gatewayClientResponse = PostePayRefundResponseDto()
      gatewayClientResponse.refundOutcome = "KO"

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
      given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))
      given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
        .willReturn(Mono.empty())
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
      verify(paymentGatewayClient, times(1)).requestRefund(any())
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
          refundRetryService)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()

      val gatewayClientResponse = PostePayRefundResponseDto()
      gatewayClientResponse.refundOutcome = "OK"

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
      given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

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
      verify(paymentGatewayClient, times(1)).requestRefund(any())
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
          refundRetryService)

      val activatedEvent = transactionActivateEvent()

      val gatewayClientResponse = PostePayRefundResponseDto()
      gatewayClientResponse.refundOutcome = "OK"

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
      given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

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
      verify(paymentGatewayClient, times(0)).requestRefund(any())
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
          refundRetryService)

      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val expiredEvent =
        transactionExpiredEvent(reduceEvents(activatedEvent, authorizationRequestedEvent))
      val gatewayClientResponse = PostePayRefundResponseDto()
      gatewayClientResponse.refundOutcome = "OK"

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
      given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

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
      verify(paymentGatewayClient, times(1)).requestRefund(any())
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
  fun `messageReceiver calls refund on transaction with NOTIFIED_KO`() = runTest {
    val transactionExpirationQueueConsumer =
      TransactionExpirationQueueConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils,
        refundRetryService)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val addUserReceiptEvent =
      transactionUserReceiptAddedEvent(TransactionUserReceiptData.Outcome.KO)
    val expiredEvent =
      transactionExpiredEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closedEvent,
          addUserReceiptEvent))

    val gatewayClientResponse = PostePayRefundResponseDto()
    gatewayClientResponse.refundOutcome = "OK"

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
    given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

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
    verify(paymentGatewayClient, times(1)).requestRefund(any())
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
}
