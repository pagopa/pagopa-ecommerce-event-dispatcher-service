package it.pagopa.ecommerce.eventdispatcher.redis.streams

import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import org.slf4j.LoggerFactory
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

/**
 * Redis Stream event consumer. This class handles all Redis Stream events performing requested
 * operation based on input event type
 */
@Service
class RedisStreamConsumer() {

  private val logger = LoggerFactory.getLogger(javaClass)

  @ServiceActivator(
    inputChannel = "eventDispatcherReceiverCommandChannel", outputChannel = "nullChannel")
  fun readStreamEvent(@Payload message: Message<EventDispatcherGenericCommand>) {
    logger.info("Received event: {}", message)
    when (val command = message.payload) {
      is EventDispatcherReceiverCommand ->
        println("RECEIVED receiver command with input command ${command.receiverCommand}")
      else -> println("Unmanaged command of type: ${command.javaClass}")
    }
  }
}
