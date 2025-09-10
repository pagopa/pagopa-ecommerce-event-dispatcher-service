package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.commons.redis.reactivetemplatewrappers.ReactiveRedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import java.time.Duration
import org.springframework.data.redis.core.ReactiveRedisTemplate

/** Redis command template wrapper, used to write events to Redis stream */
class EventDispatcherCommandsTemplateWrapper(
  redisTemplate: ReactiveRedisTemplate<String, EventDispatcherReceiverCommand>,
  defaultEntitiesTTL: Duration
) :
  ReactiveRedisTemplateWrapper<EventDispatcherReceiverCommand>(
    redisTemplate, "eventDispatcher", defaultEntitiesTTL) {
  override fun getKeyFromEntity(value: EventDispatcherReceiverCommand): String =
    value.commandId.toString()
}
