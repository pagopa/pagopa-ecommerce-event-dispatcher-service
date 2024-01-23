package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.commons.redis.templatewrappers.PaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.commons.redis.templatewrappers.RedisTemplateWrapperBuilder
import it.pagopa.ecommerce.eventdispatcher.config.redis.stream.EventDispatcherCommandsTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import java.time.Duration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.serializer.StringRedisSerializer

@Configuration
class RedisConfig {
  @Bean
  fun paymentRequestInfoWrapper(
    redisConnectionFactory: RedisConnectionFactory
  ): PaymentRequestInfoRedisTemplateWrapper {
    // PaymentRequestInfo entities will have the same TTL
    // value
    return RedisTemplateWrapperBuilder.buildPaymentRequestInfoRedisTemplateWrapper(
      redisConnectionFactory, Duration.ofMinutes(10))
  }

  @Bean
  fun eventDispatcherCommandRedisTemplateWrapper(
    redisConnectionFactory: RedisConnectionFactory
  ): EventDispatcherCommandsTemplateWrapper {
    val redisTemplate = RedisTemplate<String, EventDispatcherReceiverCommand>()
    redisTemplate.setConnectionFactory(redisConnectionFactory)
    redisTemplate.setDefaultSerializer(StringRedisSerializer())
    redisTemplate.afterPropertiesSet()
    return EventDispatcherCommandsTemplateWrapper(redisTemplate, "eventDispatcher", Duration.ZERO)
  }
}
