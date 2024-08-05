package it.pagopa.ecommerce.eventdispatcher.redis.streams

import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.messaging.Message
import org.springframework.messaging.support.GenericMessage

class RedisStreamConsumerTest {

  private val deploymentVersion = DeploymentVersionDto.NEW

  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService =
    mock()

  private val redisStreamConsumer =
    RedisStreamConsumer(
      inboundChannelAdapterLifecycleHandlerService = inboundChannelAdapterLifecycleHandlerService,
      deploymentVersion = deploymentVersion)

  @ParameterizedTest
  @EnumSource(EventDispatcherReceiverCommand.ReceiverCommand::class)
  fun `Should handle receiver event successfully`(
    command: EventDispatcherReceiverCommand.ReceiverCommand
  ) {
    // pre-requisite
    val eventMessage =
      GenericMessage(
        EventDispatcherReceiverCommand(
          receiverCommand = command, version = DeploymentVersionDto.NEW))
        as Message<EventDispatcherGenericCommand>
    // test
    redisStreamConsumer.readStreamEvent(eventMessage)
    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService, times(1))
      .invokeCommandForAllEndpoints(command.toString().lowercase())
  }

  @Test
  fun `Should throw exception for unmanaged event`() {
    // pre-requisite
    val event: EventDispatcherGenericCommand = mock()
    val eventMessage = GenericMessage(event) as Message<EventDispatcherGenericCommand>
    // test
    assertThrows<RuntimeException> { redisStreamConsumer.readStreamEvent(eventMessage) }

    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService, times(0))
      .invokeCommandForAllEndpoints(any())
  }

  @ParameterizedTest
  @EnumSource(EventDispatcherReceiverCommand.ReceiverCommand::class)
  fun `Should ignore event for command target version different than current version`(
    command: EventDispatcherReceiverCommand.ReceiverCommand
  ) {
    // pre-requisite
    val eventMessage =
      GenericMessage(
        EventDispatcherReceiverCommand(
          receiverCommand = command,
          version = DeploymentVersionDto.values().first { it != deploymentVersion }))
        as Message<EventDispatcherGenericCommand>
    // test
    redisStreamConsumer.readStreamEvent(eventMessage)
    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService, times(0))
      .invokeCommandForAllEndpoints(any())
  }
}
