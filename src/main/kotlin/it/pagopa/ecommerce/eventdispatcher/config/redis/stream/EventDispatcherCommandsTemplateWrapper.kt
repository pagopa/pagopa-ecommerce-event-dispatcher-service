package it.pagopa.ecommerce.eventdispatcher.config.redis.stream

import it.pagopa.ecommerce.commons.redis.templatewrappers.RedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import java.time.Duration
import org.springframework.data.redis.core.RedisTemplate

class EventDispatcherCommandsTemplateWrapper(
  redisTemplateWrapper: RedisTemplate<String, EventDispatcherReceiverCommand>,
  keyspace: String,
  defaultEntitiesTTL: Duration
) :
  RedisTemplateWrapper<EventDispatcherReceiverCommand>(
    redisTemplateWrapper, keyspace, defaultEntitiesTTL) {
  override fun getKeyFromEntity(value: EventDispatcherReceiverCommand): String =
    value.commandId.toString()
}
