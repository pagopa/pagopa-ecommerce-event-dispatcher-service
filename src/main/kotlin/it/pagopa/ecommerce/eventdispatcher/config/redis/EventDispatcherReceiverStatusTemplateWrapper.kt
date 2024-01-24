package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.commons.redis.templatewrappers.RedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiversStatus
import java.time.Duration
import org.springframework.data.redis.core.RedisTemplate

/** Redis template wrapper used to handle event receiver statuses */
class EventDispatcherReceiverStatusTemplateWrapper(
  redisTemplateWrapper: RedisTemplate<String, ReceiversStatus>,
  defaultEntitiesTTL: Duration
) :
  RedisTemplateWrapper<ReceiversStatus>(
    redisTemplateWrapper, "receiver-status", defaultEntitiesTTL) {
  override fun getKeyFromEntity(value: ReceiversStatus): String = value.consumerInstanceId
}
