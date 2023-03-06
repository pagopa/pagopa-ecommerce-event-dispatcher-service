package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.transactionActivateEvent
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.transactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.eventretry.RefundRetryService
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionExpiredEventsConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val refundRetryService: RefundRetryService = mock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val transactionExpiredEventsConsumer =
    TransactionExpiredEventsConsumer(
      paymentGatewayClient,
      transactionsEventStoreRepository,
      transactionsRefundedEventStoreRepository,
      transactionsViewRepository,
      refundRetryService)

  @Test
  fun `messageReceiver receives expired messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val expiredEvent =
      TransactionExpiredEvent(transactionId, TransactionExpiredData(TransactionStatusDto.ACTIVATED))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionId(
          transactionId,
        ))
      .willReturn(
        Flux.just(activatedEvent as TransactionEvent<Any>, expiredEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }

    /* test */
    StepVerifier.create(
        transactionExpiredEventsConsumer.messageReceiver(
          BinaryData.fromObject(expiredEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives refund messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val expiredEvent =
      TransactionExpiredEvent(transactionId, TransactionExpiredData(TransactionStatusDto.ACTIVATED))

    val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(0)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionId(
          transactionId,
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          expiredEvent as TransactionEvent<Any>,
          refundRetriedEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }

    /* test */
    StepVerifier.create(
        transactionExpiredEventsConsumer.messageReceiver(
          BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()

    val expiredEvent =
      TransactionExpiredEvent(transactionId, TransactionExpiredData(TransactionStatusDto.ACTIVATED))

    val transaction =
      Transaction(
        transactionId,
        activatedEvent.data.paymentNotices,
        activatedEvent.data.paymentNotices.sumOf { it.amount },
        activatedEvent.data.email,
        TransactionStatusDto.EXPIRED,
        activatedEvent.data.clientId,
        activatedEvent.creationDate)

    val gatewayClientResponse = PostePayRefundResponseDto().apply { refundOutcome = "OK" }

    val refundedEvent =
      TransactionRefundedEvent(
        transactionId, TransactionRefundedData(TransactionStatusDto.ACTIVATED))

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
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture()))
      .willReturn(Mono.just(transaction))
    given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

    /* test */
    StepVerifier.create(
        transactionExpiredEventsConsumer.messageReceiver(
          BinaryData.fromObject(expiredEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestRefund(any())
    val expectedRefundEventStatuses =
      listOf(
        TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        TransactionEventCode.TRANSACTION_REFUNDED_EVENT)
    val viewExpectedStatuses =
      listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUNDED)
    viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
      Assertions.assertEquals(
        expectedStatus,
        transactionViewRepositoryCaptor.allValues[idx].status,
        "Unexpected view status on idx: $idx")
    }
    expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
      Assertions.assertEquals(
        expectedStatus,
        transactionRefundEventStoreCaptor.allValues[idx].eventCode,
        "Unexpected event code on idx: $idx")
    }
  }

  @Test
  fun `messageReceiver doesn't call refund on event saving error`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val expiredEvent =
      TransactionExpiredEvent(transactionId, TransactionExpiredData(TransactionStatusDto.ACTIVATED))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionId(
          any(),
        ))
      .willReturn(
        Flux.just(activatedEvent as TransactionEvent<Any>, expiredEvent as TransactionEvent<Any>))

    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.empty())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }

    /* test */
    StepVerifier.create(
        transactionExpiredEventsConsumer.messageReceiver(
          BinaryData.fromObject(expiredEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(0)).requestRefund(any())
  }

  @Test
  fun `messageReceiver refunds correctly with KO outcome from gateway`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()

    val expiredEvent =
      TransactionExpiredEvent(transactionId, TransactionExpiredData(TransactionStatusDto.ACTIVATED))

    val transaction =
      Transaction(
        transactionId,
        activatedEvent.data.paymentNotices,
        activatedEvent.data.paymentNotices.sumOf { it.amount },
        activatedEvent.data.email,
        TransactionStatusDto.EXPIRED,
        activatedEvent.data.clientId,
        activatedEvent.creationDate)

    val gatewayClientResponse = PostePayRefundResponseDto().apply { refundOutcome = "KO" }

    val refundedEvent =
      TransactionRefundedEvent(
        transactionId, TransactionRefundedData(TransactionStatusDto.ACTIVATED))

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
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture()))
      .willReturn(Mono.just(transaction))
    given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
      .willReturn(Mono.empty())
    /* test */
    StepVerifier.create(
        transactionExpiredEventsConsumer.messageReceiver(
          BinaryData.fromObject(expiredEvent).toBytes(), checkpointer))
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestRefund(any())
    val expectedRefundEventStatuses =
      listOf(
        TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT,
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT)
    val viewExpectedStatuses =
      listOf(TransactionStatusDto.REFUND_REQUESTED, TransactionStatusDto.REFUND_ERROR)
    viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
      Assertions.assertEquals(
        expectedStatus,
        transactionViewRepositoryCaptor.allValues[idx].status,
        "Unexpected view status on idx: $idx")
    }
    expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
      Assertions.assertEquals(
        expectedStatus,
        transactionRefundEventStoreCaptor.allValues[idx].eventCode,
        "Unexpected event code on idx: $idx")
    }
    Assertions.assertEquals(0, retryCountCaptor.value)
  }
}
