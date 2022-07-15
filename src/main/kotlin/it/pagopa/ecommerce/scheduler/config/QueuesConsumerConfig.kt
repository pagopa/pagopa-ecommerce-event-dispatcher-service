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
    private val queueName: String? = null

    @Bean
    fun input() : DirectChannel {
        return DirectChannel()
    }

    @Bean
    @InboundChannelAdapter(channel = "transactionauthrequestedchannel", poller = [Poller(fixedDelay = "1000")])
    fun storageQueueMessageSource(storageQueueTemplate: StorageQueueTemplate): StorageQueueMessageSource {
        return StorageQueueMessageSource(queueName, storageQueueTemplate)
    }
}