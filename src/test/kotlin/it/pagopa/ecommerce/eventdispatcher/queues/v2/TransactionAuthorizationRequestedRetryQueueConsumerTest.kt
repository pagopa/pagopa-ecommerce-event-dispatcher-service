package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.time.OffsetDateTime
import java.util.stream.Stream
import org.junit.jupiter.params.provider.Arguments
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.kotlin.*

class TransactionAuthorizationRequestedRetryQueueConsumerTest {

  private val transactionsServiceClient: TransactionsServiceClient = mock()

  private val transactionsEventStoreRepository:
    TransactionsEventStoreRepository<TransactionAuthorizationRequestedRetriedEvent> =
    mock()

  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService =
    mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val tracingUtils = TracingUtilsTests.getMock()

  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()

  private val checkpointer: Checkpointer = mock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionAuthorizationCompletedStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionAuthorizationCompletedData>>

  @Captor private lateinit var retryCountCaptor: ArgumentCaptor<Int>

  private val transactionAuthorizationRequestedRetryQueueConsumer =
    TransactionAuthorizationRequestedRetryQueueConsumer(
      transactionsServiceClient = transactionsServiceClient,
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
      authorizationStateRetrieverService = authorizationStateRetrieverService,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2)

  companion object {
    @JvmStatic
    fun `Recover transaction status timestamp method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of("2024-01-01T00:00:00", OffsetDateTime.parse("2024-01-01T00:00:00+01:00")),
        Arguments.of("2024-08-01T00:00:00", OffsetDateTime.parse("2024-08-01T00:00:00+02:00")))
  }

  /*
  @Test
  fun `messageReceiver consume event correctly with PAYMENT_COMPLETE outcome from NPG`() = runTest {
    val activatedEvent = transactionActivateEvent()

    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()

    val authorizationRequestedRetriedEvent = transactionAuthorizationRequestedRetriedEvent(0)

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
         )
      )

    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(paymentGatewayClient.requestVPosRefund(any()))
      .willReturn(Mono.just(gatewayClientResponse))
    given(refundRetryService.enqueueRetryEvent(any(), retryCountCaptor.capture(), any()))
      .willReturn(Mono.empty())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(
            it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto.REFUND_ERROR, ZonedDateTime.now())
        ))
    /* test */
    StepVerifier.create(
      transactionRefundRetryQueueConsumer.messageReceiver(
        QueueEvent(refundRetriedEvent, TracingInfoTest.MOCK_TRACING_INFO), checkpointer))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(paymentGatewayClient, times(1)).requestVPosRefund(any())
    Assertions.assertEquals(
      it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto.REFUNDED,
      transactionViewRepositoryCaptor.value.status,
      "Unexpected view status")
    Assertions.assertEquals(
      TransactionEventCode.TRANSACTION_REFUNDED_EVENT,
      TransactionEventCode.valueOf(transactionAuthorizationCompletedStoreCaptor.value.eventCode),
      "Unexpected event code")
    verify(refundRetryService, times(0)).enqueueRetryEvent(any(), any(), any())
  }*/

}
