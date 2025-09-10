package it.pagopa.ecommerce.eventdispatcher.config.redis

import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import java.time.Duration
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.ReactiveValueOperations
import reactor.core.publisher.Mono

class EventDispatcherCommandsTemplateWrapperTest {
  private val redisTemplate: ReactiveRedisTemplate<String, EventDispatcherReceiverCommand> = mock()

  private val opsForValue: ReactiveValueOperations<String, EventDispatcherReceiverCommand> = mock()

  private val defaultTTL = Duration.ofSeconds(1)

  private val eventDispatcherCommandsTemplateWrapper =
    EventDispatcherCommandsTemplateWrapper(
      redisTemplate = redisTemplate, defaultEntitiesTTL = defaultTTL)

  @Test
  fun `Should save entity successfully`() {
    val eventDispatcherCommand =
      EventDispatcherReceiverCommand(
        receiverCommand = EventDispatcherReceiverCommand.ReceiverCommand.START,
        version = DeploymentVersionDto.PROD)
    given(redisTemplate.opsForValue()).willReturn(opsForValue)
    given(opsForValue.set(any(), any(), any<Duration>())).willReturn(Mono.just(true))
    eventDispatcherCommandsTemplateWrapper.save(eventDispatcherCommand)
    verify(opsForValue, times(1))
      .set(
        "eventDispatcher:${eventDispatcherCommand.commandId}", eventDispatcherCommand, defaultTTL)
  }
}
