package it.pagopa.ecommerce.scheduler.config

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
    return QueueClientBuilder()
      .connectionString(storageConnectionString)
      .queueName(queueEventInitName)
      .buildAsyncClient()
  }

  @Bean
  fun closureRetryQueueAsyncClient(
    @Value("\${azurestorage.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionclosepaymentretry.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return QueueClientBuilder()
      .connectionString(storageConnectionString)
      .queueName(queueEventInitName)
      .buildAsyncClient()
  }
}
