package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiversStatus
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import java.time.Duration
import java.time.ZonedDateTime
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.ValueOperations

class EventDispatcherReceiverStatusTemplateWrapperTest {

  private val redisTemplate: RedisTemplate<String, ReceiversStatus> = mock()

  private val opsForValue: ValueOperations<String, ReceiversStatus> = mock()

  private val defaultTTL = Duration.ofSeconds(1)

  private val eventDispatcherReceiverStatusTemplateWrapper =
    EventDispatcherReceiverStatusTemplateWrapper(
      redisTemplate = redisTemplate, defaultEntitiesTTL = defaultTTL)

  @Test
  fun `Should save entity successfully`() {
    val consumerId = "uniqueConsumerId"
    val receiverStatus =
      ReceiversStatus(
        receiverStatuses = listOf(),
        queriedAt = ZonedDateTime.now().toString(),
        consumerInstanceId = consumerId,
        version = DeploymentVersionDto.NEW)
    given(redisTemplate.opsForValue()).willReturn(opsForValue)
    doNothing().`when`(opsForValue).set(any(), any(), any<Duration>())
    eventDispatcherReceiverStatusTemplateWrapper.save(receiverStatus)
    verify(opsForValue, times(1)).set("receiver-status:$consumerId", receiverStatus, defaultTTL)
  }
}
