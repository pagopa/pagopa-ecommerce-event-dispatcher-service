package it.pagopa.ecommerce.eventdispatcher.redis.streams

import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
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
    InboundChannelAdapterLifecycleHandlerService,
  @Value("\${eventController.deploymentVersion}")
  private val deploymentVersion: DeploymentVersionDto
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  @ServiceActivator(
    inputChannel = "eventDispatcherReceiverCommandChannel", outputChannel = "nullChannel")
  fun readStreamEvent(@Payload message: Message<EventDispatcherGenericCommand>) {
    logger.info("Received event: {}", message)
    when (val command = message.payload) {
      is EventDispatcherReceiverCommand -> handleEventReceiverCommand(command)
      else -> throw RuntimeException("Unhandled command received: $command")
    }
  }

  /** Handle event receiver command to start/stop receivers */
  private fun handleEventReceiverCommand(command: EventDispatcherReceiverCommand) {
    // current deployment version is targeted by command for exact version match or if command does
    // not explicit a targeted version
    val currentDeploymentVersion = deploymentVersion
    val commandTargetVersion = command.version
    val isTargetedByCommand =
      commandTargetVersion == null || currentDeploymentVersion == commandTargetVersion
    logger.info(
      "Event dispatcher receiver command event received. Current deployment version: [{}], command deployment version: [{}] -> is this version targeted: [{}]",
      currentDeploymentVersion,
      commandTargetVersion ?: "ALL",
      isTargetedByCommand)
    if (isTargetedByCommand) {
      val commandToSend =
        when (command.receiverCommand) {
          EventDispatcherReceiverCommand.ReceiverCommand.START -> "start"
          EventDispatcherReceiverCommand.ReceiverCommand.STOP -> "stop"
        }
      inboundChannelAdapterLifecycleHandlerService.invokeCommandForAllEndpoints(commandToSend)
    } else {
      logger.info(
        "Current deployment version not targeted by command, command will not be processed")
    }
  }
}
