package it.pagopa.ecommerce.eventdispatcher.warmup

import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.validation.BeanValidationConfiguration
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource

@Import(BeanValidationConfiguration::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class ServicesRequestsTest {

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()
  private val queuesConsumerConfig = QueuesConsumerConfig()

  val strictSerializerProviderV1 = queuesConsumerConfig.strictSerializerProviderV1()
  val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()

  private val transactionRefundedEventsConsumer =
    TransactionsRefundQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionRefundedRetryEventsConsumer =
    TransactionRefundRetryQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionNotificationsRetryQueueConsumer =
    TransactionNotificationsRetryQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionClosePaymentRetryQueueConsumer =
    TransactionClosePaymentRetryQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionClosePaymentQueueConsumer =
    TransactionClosePaymentQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionExpirationQueueConsumer =
    TransactionExpirationQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionAuthorizationRequestedQueueConsumer =
    TransactionAuthorizationRequestedQueueConsumer(
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  @Test
  fun `test warmupService is blocking`() {

    // Call the warmupService method
    transactionRefundedEventsConsumer.warmupService()
  }
}
