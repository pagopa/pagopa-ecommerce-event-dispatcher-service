package it.pagopa.ecommerce.scheduler.config

import com.azure.spring.integration.storage.queue.inbound.StorageQueueMessageSource
import com.azure.spring.messaging.storage.queue.core.StorageQueueTemplate
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.annotation.Poller

@Configuration
class QueuesConsumerConfig {
  @Bean
  @InboundChannelAdapter(
    channel = "transactionactivatedchannel", poller = [Poller(fixedDelay = "1000")])
  fun storageQueueActivatedMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionactivatedevents.name}")
    queueNameActivatedEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameActivatedEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionclosureschannel", poller = [Poller(fixedDelay = "1000")])
  fun storageQueueClosuresMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionclosuresentevents.name}")
    queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionexpiredchannel", poller = [Poller(fixedDelay = "1000")])
  fun storageQueueExpirationsMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionexpiredevents.name}") queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionsrefundchannel", poller = [Poller(fixedDelay = "1000")])
  fun storageQueueRefundMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionsrefund.name}") refundRequestedEvent: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(refundRequestedEvent, storageQueueTemplate)
  }
}
