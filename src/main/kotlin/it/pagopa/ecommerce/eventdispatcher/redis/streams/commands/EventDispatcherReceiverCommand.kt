package it.pagopa.ecommerce.eventdispatcher.redis.streams.commands

import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto

/** Event dispatcher command event used to start/stop all receivers */
data class EventDispatcherReceiverCommand(
  val receiverCommand: ReceiverCommand,
  val version: DeploymentVersionDto?
) : EventDispatcherGenericCommand(type = CommandType.RECEIVER_COMMAND) {

  /** Enumeration of all possible actions for event receivers */
  enum class ReceiverCommand {
    START,
    STOP
  }
}
