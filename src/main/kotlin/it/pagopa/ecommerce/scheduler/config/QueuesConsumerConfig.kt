package it.pagopa.ecommerce.scheduler.config

import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Bean
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.annotation.Poller
import com.azure.spring.integration.storage.queue.inbound.StorageQueueMessageSource
import com.azure.spring.messaging.storage.queue.core.StorageQueueTemplate
import org.springframework.beans.factory.annotation.Value


@Configuration
class QueuesConsumerConfig {

    @Value("\${azurestorage.queues.transactionauthrequestedevents.name}")
    private val queueNameAuthRequestedEvents: String? = null

    @Value("\${azurestorage.queues.transactionactivatedevents.name}")
    private val queueNameActivatedEvents: String? = null

    @Bean
    fun input(): DirectChannel {
        return DirectChannel()
    }

 /**
    @Bean
    @InboundChannelAdapter(channel = "transactionauthrequestedchannel", poller = [Poller(fixedDelay = "1000")])
    fun storageQueueAuthMessageSource(storageQueueTemplate: StorageQueueTemplate): StorageQueueMessageSource {
        return StorageQueueMessageSource(queueNameAuthRequestedEvents, storageQueueTemplate)
    }
*/
    @Bean
    @InboundChannelAdapter(channel = "transactionactivatedchannel", poller = [Poller(fixedDelay = "1000")])
    fun storageQueueActivatedMessageSource(storageQueueTemplate: StorageQueueTemplate): StorageQueueMessageSource {
        return StorageQueueMessageSource(queueNameActivatedEvents, storageQueueTemplate)
    }
}