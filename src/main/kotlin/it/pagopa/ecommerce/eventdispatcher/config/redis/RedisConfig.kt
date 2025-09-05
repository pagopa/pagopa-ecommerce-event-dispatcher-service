package it.pagopa.ecommerce.eventdispatcher.config.redis

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.pagopa.ecommerce.commons.redis.reactivetemplatewrappers.v2.ReactivePaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.commons.redis.reactivetemplatewrappers.v2.ReactiveRedisTemplateWrapperBuilder
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiversStatus
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import java.time.Duration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

/** Redis templates wrapper configuration */
@Configuration
class RedisConfig {
  @Bean
  fun paymentRequestInfoWrapper(
    redisConnectionFactory: ReactiveRedisConnectionFactory
  ): ReactivePaymentRequestInfoRedisTemplateWrapper {
    return ReactiveRedisTemplateWrapperBuilder.buildPaymentRequestInfoRedisTemplateWrapper(
      redisConnectionFactory, Duration.ofMinutes(10))
  }

  @Bean
  fun eventDispatcherCommandRedisTemplateWrapper(
    redisConnectionFactory: ReactiveRedisConnectionFactory
  ): EventDispatcherCommandsTemplateWrapper {
    val jacksonRedisSerializer =
      Jackson2JsonRedisSerializer(jacksonObjectMapper(), EventDispatcherReceiverCommand::class.java)
    val serializationContext =
      RedisSerializationContext.newSerializationContext<String, EventDispatcherReceiverCommand>(
          StringRedisSerializer())
        .value(jacksonRedisSerializer)
        .build()

    val reactiveRedisTemplate = ReactiveRedisTemplate(redisConnectionFactory, serializationContext)
    /*
     * This redis template instance is to write events to Redis Stream through opsForStreams apis.
     * No document is written into cache.
     * Set TTL to 0 here will throw an error during writing operation to cache to enforce the fact that this
     * wrapper has to be used only to write to Redis Streams
     */
    return EventDispatcherCommandsTemplateWrapper(reactiveRedisTemplate, Duration.ZERO)
  }

  @Bean
  fun eventDispatcherReceiverStatusTemplateWrapper(
    redisConnectionFactory: ReactiveRedisConnectionFactory
  ): EventDispatcherReceiverStatusTemplateWrapper {

    val jacksonRedisSerializer =
      Jackson2JsonRedisSerializer(jacksonObjectMapper(), ReceiversStatus::class.java)
    val serializationContext =
      RedisSerializationContext.newSerializationContext<String, ReceiversStatus>(
          StringRedisSerializer())
        .value(jacksonRedisSerializer)
        .build()

    val reactiveRedisTemplate = ReactiveRedisTemplate(redisConnectionFactory, serializationContext)
    return EventDispatcherReceiverStatusTemplateWrapper(
      reactiveRedisTemplate, Duration.ofMinutes(1))
  }
}
