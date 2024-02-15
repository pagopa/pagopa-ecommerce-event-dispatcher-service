package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRetriedData
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import java.time.ZonedDateTime
import java.util.stream.Stream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.mock

class AuthorizationStateRetrieverRetryServiceTest {

  private val paymentTokenValidityTimeOffset = 10

  private val authRequestedQueueAsyncClient: QueueAsyncClient = mock()

  private val viewRepository: TransactionsViewRepository = mock()

  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData> =
    mock()
  private val retryOffset: Int = 10
  private val maxAttempts: Int = 3

  private val transientQueuesTTLSeconds: Int = 10

  private val strictSerializerProviderV2: StrictJsonSerializerProvider =
    QueuesConsumerConfig().strictSerializerProviderV2()

  private val authorizationStateRetrieverRetryService =
    AuthorizationStateRetrieverRetryService(
      paymentTokenValidityTimeOffset = paymentTokenValidityTimeOffset,
      authRequestedQueueAsyncClient = authRequestedQueueAsyncClient,
      viewRepository = viewRepository,
      eventStoreRepository = eventStoreRepository,
      retryOffset = retryOffset,
      maxAttempts = maxAttempts,
      transientQueuesTTLSeconds = transientQueuesTTLSeconds,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  @Test
  fun `Should throw exception trying to enqueue a retry event`() {
    val events: List<TransactionEvent<Any>> =
      listOf(
        TransactionTestUtils.transactionActivateEvent() as TransactionEvent<Any>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<Any>)

    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    assertThrows<NotImplementedError> {
      authorizationStateRetrieverRetryService.enqueueRetryEvent(
        baseTransaction = baseTransaction, retriedCount = 0, tracingInfo = null)
    }
  }

  companion object {
    @JvmStatic
    fun `Visibility timeout validation test method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          ZonedDateTime.now().minus(Duration.ofMinutes(10)).toString(),
          Duration.ofMinutes(9),
          false),
        Arguments.of(
          ZonedDateTime.now().plus(Duration.ofMinutes(10)).toString(), Duration.ofMinutes(9), true),
      )
  }

  @ParameterizedTest
  @MethodSource("Visibility timeout validation test method source")
  fun `Should validate visibility timeout correctly`(
    creationDate: String,
    visibilityTimeout: Duration,
    expectedResult: Boolean
  ) {
    // pre-requisites
    val transactionActivatedEvent =
      TransactionTestUtils.transactionActivateEvent(
        creationDate, TransactionTestUtils.npgTransactionGatewayActivationData())
    val transactionAuthorizationRequestedEvent =
      TransactionTestUtils.transactionAuthorizationRequestedEvent()
    val events: List<TransactionEvent<Any>> =
      listOf(
        transactionActivatedEvent as TransactionEvent<Any>,
        transactionAuthorizationRequestedEvent as TransactionEvent<Any>)
    val baseTransaction = TransactionTestUtils.reduceEvents(*events.toTypedArray())
    // test
    val visibilityTimeoutCheck =
      authorizationStateRetrieverRetryService.validateRetryEventVisibilityTimeout(
        baseTransaction = baseTransaction, visibilityTimeout)
    assertEquals(expectedResult, visibilityTimeoutCheck)
  }
}
