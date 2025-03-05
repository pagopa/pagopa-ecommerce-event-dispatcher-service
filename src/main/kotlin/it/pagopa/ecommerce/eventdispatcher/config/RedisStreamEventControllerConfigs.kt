package it.pagopa.ecommerce.eventdispatcher.config

import java.util.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration

@Configuration
class RedisStreamEventControllerConfigs(
  @Value("\${redisStream.eventController.streamKey}") val streamKey: String
) {
  private val uniqueInstanceId = UUID.randomUUID().toString()

  val instanceId: String = "event-dispatcher-$uniqueInstanceId"
}
