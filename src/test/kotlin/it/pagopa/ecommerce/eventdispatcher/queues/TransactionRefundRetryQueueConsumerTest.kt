package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import java.time.ZonedDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionRefundRetryQueueConsumerTest {

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()
  private val checkpointer: Checkpointer = mock()

  private val refundRetryService: RefundRetryService = mock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionRefundedData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionRefundRetryQueueConsumer =
    TransactionRefundRetryQueueConsumer(
      paymentGatewayClient,
      transactionsEventStoreRepository,
      transactionsRefundedEventStoreRepository,
      transactionsViewRepository,
      refundRetryService,
      deadLetterQueueAsyncClient)

  @Test
  fun `messageReceiver consume event correctly with OK outcome from gateway`() = runTest {
    val activatedEvent = TransactionTestUtils.transactionActivateEvent()

    val authorizationRequestedEvent = TransactionTestUtils.transactionAuthorizationRequestedEvent()

    val expiredEvent =
      TransactionTestUtils.transactionExpiredEvent(
        TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
    val refundRequestedEvent =
      TransactionTestUtils.transactionRefundRequestedEvent(
        TransactionTestUtils.reduceEvents(
          activatedEvent, authorizationRequestedEvent, expiredEvent))
    val refundErrorEvent =
      TransactionTestUtils.transactionRefundErrorEvent(
        TransactionTestUtils.reduceEvents(
          activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
    val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(0)

    val gatewayClientResponse =
      VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.CANCELLED }

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
          expiredEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>,
          refundRetriedEvent as TransactionEvent<Any>,
          refundErrorEvent as TransactionEvent<Any>))

    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
      .willReturn(
        Mono.just(
          TransactionTestUtils.transactionDocument(
            TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
    /* test */
    StepVerifier.create(
        transactionRefundRetryQueueConsumer.messageReceiver(
          BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
    assertEquals(
      TransactionStatusDto.REFUNDED,
      transactionViewRepositoryCaptor.value.status,
      "Unexpected view status")
    assertEquals(
      TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
      transactionRefundEventStoreCaptor.value.eventCode,
      "Unexpected event code")
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
  }

  @Test
  fun `messageReceiver consume event correctly with KO outcome from gateway writing refund error event`() =
    runTest {
      val retryCount = 0
      val activatedEvent = TransactionTestUtils.transactionActivateEvent()

      val authorizationRequestedEvent =
        TransactionTestUtils.transactionAuthorizationRequestedEvent()

      val expiredEvent =
        TransactionTestUtils.transactionExpiredEvent(
          TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
      val refundRequestedEvent =
        TransactionTestUtils.transactionRefundRequestedEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent))
      val refundErrorEvent =
        TransactionTestUtils.transactionRefundErrorEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(retryCount)

      val gatewayClientResponse =
        VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.CREATED }

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
            expiredEvent as TransactionEvent<Any>,
            refundRequestedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            refundErrorEvent as TransactionEvent<Any>))

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
      given(transactionsViewRepository.findByTransactionId(TransactionTestUtils.TRANSACTION_ID))
        .willReturn(
          Mono.just(
            TransactionTestUtils.transactionDocument(
              TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())))
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any())
      assertEquals(retryCount, retryCountCaptor.value)
      assertEquals(TransactionStatusDto.REFUND_ERROR, transactionViewRepositoryCaptor.value.status)
      assertEquals(
        TransactionEventCode.TRANSACTION_REFUND_ERROR_EVENT,
        transactionRefundEventStoreCaptor.value.eventCode)
    }

  @Test
  fun `messageReceiver consume event correctly with KO outcome from gateway not writing refund error event for retried event`() =
    runTest {
      val retryCount = 1
      val activatedEvent = TransactionTestUtils.transactionActivateEvent()

      val authorizationRequestedEvent =
        TransactionTestUtils.transactionAuthorizationRequestedEvent()

      val expiredEvent =
        TransactionTestUtils.transactionExpiredEvent(
          TransactionTestUtils.reduceEvents(activatedEvent, authorizationRequestedEvent))
      val refundRequestedEvent =
        TransactionTestUtils.transactionRefundRequestedEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent))
      val refundErrorEvent =
        TransactionTestUtils.transactionRefundErrorEvent(
          TransactionTestUtils.reduceEvents(
            activatedEvent, authorizationRequestedEvent, expiredEvent, refundRequestedEvent))
      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(retryCount)

      val gatewayClientResponse =
        VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.CREATED }

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
            expiredEvent as TransactionEvent<Any>,
            refundRequestedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
            refundErrorEvent as TransactionEvent<Any>))

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
      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(refundRetryService, times(1)).enqueueRetryEvent(any(), any())
      assertEquals(retryCount, retryCountCaptor.value)
    }

  @Test
  fun `messageReceiver consume event aborting operation for transaction in unexpected state`() =
    runTest {
      val activatedEvent = TransactionTestUtils.transactionActivateEvent()

      val refundRetriedEvent = TransactionTestUtils.transactionRefundRetriedEvent(1)

      val gatewayClientResponse =
        VposDeleteResponseDto().apply { status = VposDeleteResponseDto.StatusEnum.CANCELLED }

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            refundRetriedEvent as TransactionEvent<Any>,
          ))

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
      given(
          deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())

      /* test */
      StepVerifier.create(
          transactionRefundRetryQueueConsumer.messageReceiver(
            BinaryData.fromObject(refundRetriedEvent).toBytes(), checkpointer))
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(paymentGatewayClient, times(0)).requestVPosRefund(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any())
      verify(deadLetterQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            this.toObject(TransactionRefundRetriedEvent::class.java).eventCode ==
              TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT
          },
          any(),
          anyOrNull())
    }
}
