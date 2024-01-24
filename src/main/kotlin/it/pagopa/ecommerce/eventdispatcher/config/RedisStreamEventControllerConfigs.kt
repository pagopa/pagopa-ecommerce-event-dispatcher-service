package it.pagopa.ecommerce.eventdispatcher.config

import java.util.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration

@Configuration
class RedisStreamEventControllerConfig(
  @Value("\${redisStream.eventController.streamKey}") val streamKey: String,
  @Value("\${redisStream.eventController.consumerGroupPrefix}") consumerGroupPrefix: String,
  @Value("\${redisStream.eventController.consumerNamePrefix}") consumerNamePrefix: String
) {
  private val uniqueConsumerId = UUID.randomUUID().toString()
  val consumerGroup = "$consumerGroupPrefix-$uniqueConsumerId"
  val consumerName = "$consumerNamePrefix-$uniqueConsumerId"
}
