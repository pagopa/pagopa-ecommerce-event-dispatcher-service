package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.commons.redis.templatewrappers.RedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiverStatus
import java.time.Duration
import org.springframework.data.redis.core.RedisTemplate

/** Redis template wrapper used to handle event receiver statuses */
class EventDispatcherReceiverStatusTemplateWrapper(
  redisTemplateWrapper: RedisTemplate<String, ReceiverStatus>,
  defaultEntitiesTTL: Duration
) :
  RedisTemplateWrapper<ReceiverStatus>(
    redisTemplateWrapper, "receiver-status", defaultEntitiesTTL) {
  override fun getKeyFromEntity(value: ReceiverStatus): String = value.receiverName
}
