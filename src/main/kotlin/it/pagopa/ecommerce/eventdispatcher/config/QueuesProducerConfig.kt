package it.pagopa.ecommerce.eventdispatcher.config

import com.azure.storage.queue.QueueAsyncClient
import com.azure.storage.queue.QueueClientBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class QueuesProducerConfig {

  @Bean
  fun refundRetryQueueAsyncClient(
    @Value("\${azurestorage.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionrefundretry.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun closureRetryQueueAsyncClient(
    @Value("\${azurestorage.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionclosepaymentretry.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun notificationRetryQueueAsyncClient(
    @Value("\${azurestorage.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionclosepaymentretry.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  private fun buildQueueAsyncClient(storageConnectionString: String, queueName: String) =
    QueueClientBuilder()
      .connectionString(storageConnectionString)
      .queueName(queueName)
      .buildAsyncClient()
}
