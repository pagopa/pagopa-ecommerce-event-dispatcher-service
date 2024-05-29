package it.pagopa.ecommerce.eventdispatcher.config

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder
import com.azure.core.util.serializer.JsonSerializer
import com.azure.storage.queue.QueueAsyncClient
import com.azure.storage.queue.QueueClientBuilder
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.mixin.serialization.v1.QueueEventMixInEventCodeFieldDiscriminator
import it.pagopa.ecommerce.commons.queues.mixin.serialization.v2.QueueEventMixInClassFieldDiscriminator
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.netty.http.client.HttpClient

@Configuration
class QueuesProducerConfig {

  @Bean
  fun jsonSerializerV1(): JsonSerializer {
    return StrictJsonSerializerProvider()
      .addMixIn(QueueEvent::class.java, QueueEventMixInEventCodeFieldDiscriminator::class.java)
      .createInstance()
  }

  @Bean
  fun jsonSerializerV2(): JsonSerializer {
    return StrictJsonSerializerProvider()
      .addMixIn(QueueEvent::class.java, QueueEventMixInClassFieldDiscriminator::class.java)
      .createInstance()
  }

  @Bean
  fun refundQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionsrefund.name}") queueEventInitName: String,
  ): it.pagopa.ecommerce.commons.client.QueueAsyncClient {
    return it.pagopa.ecommerce.commons.client.QueueAsyncClient(
      buildQueueAsyncClient(
        storageConnectionString,
        queueEventInitName
      ),
      jsonSerializerV2()
    )
  }

  @Bean
  fun refundRetryQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionrefundretry.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun closureRetryQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionclosepaymentretry.name}") queueEventInitName: String
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun notificationRetryQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionnotificationretry.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun deadLetterQueueAsyncClient(
    @Value("\${azurestorage.deadletter.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.deadletter.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun expirationQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionexpiration.name}") queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun authRequestedQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionauthorizationrequested.name}")
    queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  @Bean
  fun authRequestedOutcomeWaitingQueueAsyncClient(
    @Value("\${azurestorage.transient.connectionstring}") storageConnectionString: String,
    @Value("\${azurestorage.queues.transactionauthorizationoutcomewaiting.name}")
    queueEventInitName: String,
  ): QueueAsyncClient {
    return buildQueueAsyncClient(storageConnectionString, queueEventInitName)
  }

  private fun buildQueueAsyncClient(storageConnectionString: String, queueName: String) =
    QueueClientBuilder()
      .connectionString(storageConnectionString)
      .queueName(queueName)
      .httpClient(NettyAsyncHttpClientBuilder(HttpClient.create().resolver { it.ndots(1) }).build())
      .buildAsyncClient()
}
