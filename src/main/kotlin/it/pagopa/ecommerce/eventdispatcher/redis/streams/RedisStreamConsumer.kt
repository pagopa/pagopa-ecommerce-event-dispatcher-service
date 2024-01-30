package it.pagopa.ecommerce.eventdispatcher.redis.streams

import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

/**
 * Redis Stream event consumer. This class handles all Redis Stream events performing requested
 * operation based on input event type
 */
@Service
class RedisStreamConsumer(
  @Autowired
  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  @ServiceActivator(
    inputChannel = "eventDispatcherReceiverCommandChannel", outputChannel = "nullChannel")
  fun readStreamEvent(@Payload message: Message<EventDispatcherGenericCommand>) {
    logger.info("Received event: {}", message)
    val commandToSend =
      when (val command = message.payload) {
        is EventDispatcherReceiverCommand ->
          when (command.receiverCommand) {
            EventDispatcherReceiverCommand.ReceiverCommand.START -> "start"
            EventDispatcherReceiverCommand.ReceiverCommand.STOP -> "stop"
          }
        else -> throw RuntimeException("Unhandled command received: $command")
      }
    inboundChannelAdapterLifecycleHandlerService.invokeCommandForAllEndpoints(commandToSend)
  }
}
