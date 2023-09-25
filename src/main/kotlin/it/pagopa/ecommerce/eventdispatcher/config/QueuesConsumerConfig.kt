package it.pagopa.ecommerce.eventdispatcher.config

import com.azure.spring.integration.storage.queue.inbound.StorageQueueMessageSource
import com.azure.spring.messaging.storage.queue.core.StorageQueueTemplate
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.mixin.deserialization.TransactionEventMixIn
import it.pagopa.ecommerce.commons.queues.mixin.serialization.QueueEventMixInClassFieldDiscriminator
import it.pagopa.ecommerce.commons.queues.mixin.serialization.QueueEventMixInEventCodeFieldDiscriminator
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.annotation.Poller

@Configuration
class QueuesConsumerConfig {

  @Bean
  @InboundChannelAdapter(
    channel = "transactionclosureschannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueClosuresMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionclosepayment.name}") queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionretryclosureschannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueRetryClosuresMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionclosepaymentretry.name}")
    queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionexpiredchannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueExpirationsMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionexpiration.name}") queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionrefundretrychannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueRefundRetryMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionrefundretry.name}") queueNameRefundRetryEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameRefundRetryEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionsrefundchannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueRefundMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionsrefund.name}") queueNameRefundRetryEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameRefundRetryEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionretrynotificationschannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueRetryNotificationsMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionnotificationretry.name}")
    queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "transactionnotificationschannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "10")])
  fun storageQueueNotificationsMessageSource(
    storageQueueTemplate: StorageQueueTemplate,
    @Value("\${azurestorage.queues.transactionnotification.name}") queueNameClosureEvents: String
  ): StorageQueueMessageSource {
    return StorageQueueMessageSource(queueNameClosureEvents, storageQueueTemplate)
  }

  @Bean
  fun strictSerializerProviderV1(): StrictJsonSerializerProvider =
    StrictJsonSerializerProvider()
      .addMixIn(QueueEvent::class.java, QueueEventMixInEventCodeFieldDiscriminator::class.java)

  @Bean
  fun strictSerializerProviderV2(): StrictJsonSerializerProvider =
    StrictJsonSerializerProvider()
      .addMixIn(QueueEvent::class.java, QueueEventMixInClassFieldDiscriminator::class.java)
      .addMixIn(
        it.pagopa.ecommerce.commons.documents.v2.TransactionEvent::class.java,
        TransactionEventMixIn::class.java)
}
