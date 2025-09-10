package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiversStatus
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import java.time.Duration
import java.time.ZonedDateTime
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ReactiveValueOperations
import reactor.core.publisher.Mono

class EventDispatcherReceiverStatusTemplateWrapperTest {

  private val redisTemplate: ReactiveRedisTemplate<String, ReceiversStatus> = mock()

  private val opsForValue: ReactiveValueOperations<String, ReceiversStatus> = mock()

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
        version = DeploymentVersionDto.PROD)
    given(redisTemplate.opsForValue()).willReturn(opsForValue)
    given(opsForValue.set(any(), any(), any<Duration>())).willReturn(Mono.just(true))
    eventDispatcherReceiverStatusTemplateWrapper.save(receiverStatus)
    verify(opsForValue, times(1)).set("receiver-status:$consumerId", receiverStatus, defaultTTL)
  }
}
