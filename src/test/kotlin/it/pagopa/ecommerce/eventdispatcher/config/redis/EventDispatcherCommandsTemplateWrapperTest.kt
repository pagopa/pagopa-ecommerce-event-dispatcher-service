package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import java.time.Duration
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.core.ValueOperations

class EventDispatcherCommandsTemplateWrapperTest {
  private val redisTemplate: RedisTemplate<String, EventDispatcherReceiverCommand> = mock()

  private val opsForValue: ValueOperations<String, EventDispatcherReceiverCommand> = mock()

  private val defaultTTL = Duration.ofSeconds(1)

  private val eventDispatcherCommandsTemplateWrapper =
    EventDispatcherCommandsTemplateWrapper(
      redisTemplate = redisTemplate, defaultEntitiesTTL = defaultTTL)

  @Test
  fun `Should save entity successfully`() {
    val eventDispatcherCommand =
      EventDispatcherReceiverCommand(
        receiverCommand = EventDispatcherReceiverCommand.ReceiverCommand.START,
        version = DeploymentVersionDto.NEW)
    given(redisTemplate.opsForValue()).willReturn(opsForValue)
    doNothing().`when`(opsForValue).set(any(), any(), any<Duration>())
    eventDispatcherCommandsTemplateWrapper.save(eventDispatcherCommand)
    verify(opsForValue, times(1))
      .set(
        "eventDispatcher:${eventDispatcherCommand.commandId}", eventDispatcherCommand, defaultTTL)
  }
}
