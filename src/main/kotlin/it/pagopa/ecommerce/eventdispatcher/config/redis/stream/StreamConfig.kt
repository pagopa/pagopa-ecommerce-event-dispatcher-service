package it.pagopa.ecommerce.eventdispatcher.config.redis.stream

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherCommandMixin
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import java.time.Duration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.hash.Jackson2HashMapper
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.data.redis.stream.StreamReceiver
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.annotation.Poller

@Configuration
class StreamConfig {

  @Bean
  fun redisStreamReceiver(
    reactiveRedisConnectionFactory: ReactiveRedisConnectionFactory,
  ): StreamReceiver<String, ObjectRecord<String, java.util.LinkedHashMap<*, *>>>? {
    val objectMapper =
      jacksonObjectMapper()
        .addMixIn(
          EventDispatcherGenericCommand::class.java, EventDispatcherCommandMixin::class.java)
    val streamReceiverOptions =
      StreamReceiver.StreamReceiverOptions.builder()
        .pollTimeout(Duration.ofMillis(100))
        .keySerializer(
          RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer()))
        .objectMapper(Jackson2HashMapper(objectMapper, false))
        .targetType(LinkedHashMap::class.java)
        .build()
    return StreamReceiver.create(reactiveRedisConnectionFactory, streamReceiverOptions)
  }

  @Bean
  @InboundChannelAdapter(
    channel = "eventDispatcherReceiverCommandChannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "1")])
  fun eventDispatcherReceiverCommandMessageSource(
    redisStreamMessageSource: RedisStreamMessageSource
  ): RedisStreamMessageSource = redisStreamMessageSource
}
