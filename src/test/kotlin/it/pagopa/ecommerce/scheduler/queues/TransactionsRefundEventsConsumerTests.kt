package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.transactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.time.ZonedDateTime
import java.util.*

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionsRefundEventsConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val paymentGatewayClient: PaymentGatewayClient = mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val transactionRefundedEventsConsumer =
    TransactionsRefundConsumer(
      paymentGatewayClient,
      transactionsEventStoreRepository,
      transactionsRefundedEventStoreRepository,
      transactionsViewRepository,
    )

  @Test
  fun `consumer processes refund request event correctly with pgs refund`() =
    runTest {
      val activationEvent = TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      val authorizationCompleteEvent =
        TransactionTestUtils.transactionAuthorizationCompletedEvent() as TransactionEvent<Any>
      val refundRequestedEvent = TransactionRefundRequestedEvent(
        TransactionTestUtils.TRANSACTION_ID,
        TransactionRefundedData(TransactionStatusDto.REFUND_REQUESTED)
      ) as TransactionEvent<Any>

      val gatewayClientResponse = PostePayRefundResponseDto().apply { refundOutcome = "OK" }

      val events =
        listOf(
          activationEvent, authorizationRequestEvent, authorizationCompleteEvent, refundRequestedEvent)

      // not working
      val expectedRefundedEvent =
        TransactionRefundedEvent(
          TransactionTestUtils.TRANSACTION_ID, TransactionRefundedData(TransactionStatusDto.AUTHORIZATION_COMPLETED)
        )

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(transactionsEventStoreRepository.findByTransactionId(any())).willReturn(events.toFlux())
      given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsRefundedEventStoreRepository.save(any()))
        .willReturn(Mono.just(expectedRefundedEvent))
      given(paymentGatewayClient.requestRefund(any())).willReturn(Mono.just(gatewayClientResponse))

      /* test */

      val refundedEventId = UUID.fromString(expectedRefundedEvent.id)

      Mockito.mockStatic(UUID::class.java).use { uuid ->
        uuid.`when`<Any>(UUID::randomUUID).thenReturn(refundedEventId)
        uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

        transactionRefundedEventsConsumer
          .messageReceiver(BinaryData.fromObject(refundRequestedEvent).toBytes(), checkpointer)
          .block()

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
        verify(paymentGatewayClient, Mockito.times(1))
          .requestRefund(UUID.fromString(TransactionTestUtils.TRANSACTION_ID))
        verify(transactionsRefundedEventStoreRepository, Mockito.times(1))
          .save(
            any())
      }
    }

}
