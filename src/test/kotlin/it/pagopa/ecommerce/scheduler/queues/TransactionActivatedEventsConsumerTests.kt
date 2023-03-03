package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
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
class TransactionActivatedEventsConsumerTests {

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

  @Autowired private lateinit var transactionUtils: TransactionUtils

  @Test
  fun `messageReceiver receives activated messages successfully`() {
    val transactionActivatedEventsConsumer =
      TransactionActivatedEventsConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils)

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
        transactionActivatedEventsConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives refund messages successfully`() {
    val transactionActivatedEventsConsumer =
      TransactionActivatedEventsConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils)

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
        transactionActivatedEventsConsumer.messageReceiver(
          BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
      .expectNext()
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver calls refund on transaction with authorization request`() = runTest {
    val transactionActivatedEventsConsumer =
      TransactionActivatedEventsConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(TransactionStatusDto.ACTIVATED)
    val refundedEvent = transactionRefundedEvent(TransactionStatusDto.ACTIVATED)

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
        transactionActivatedEventsConsumer.messageReceiver(
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
    val transactionActivatedEventsConsumer =
      TransactionActivatedEventsConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils)

    val activatedEvent = transactionActivateEvent()
    val expiredEvent = transactionExpiredEvent(TransactionStatusDto.ACTIVATED)

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
        transactionActivatedEventsConsumer.messageReceiver(
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
    val transactionActivatedEventsConsumer =
      TransactionActivatedEventsConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(TransactionStatusDto.ACTIVATED)
    val refundedEvent = transactionRefundedEvent(TransactionStatusDto.ACTIVATED)

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
        transactionActivatedEventsConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectError()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
  }

  @Test
  fun `messageReceiver fails to generate new refund event`() = runTest {
    val transactionActivatedEventsConsumer =
      TransactionActivatedEventsConsumer(
        paymentGatewayClient,
        transactionsEventStoreRepository,
        transactionsExpiredEventStoreRepository,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        transactionUtils)

    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(TransactionStatusDto.ACTIVATED)
    val refundedEvent = transactionRefundedEvent(TransactionStatusDto.ACTIVATED)

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
        transactionActivatedEventsConsumer.messageReceiver(
          BinaryData.fromObject(activatedEvent).toBytes(), checkpointer))
      .expectError()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
  }
}
