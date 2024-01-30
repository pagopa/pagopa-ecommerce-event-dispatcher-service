package it.pagopa.ecommerce.eventdispatcher.redis.streams.commands

/** Event dispatcher command event used to start/stop all receivers */
data class EventDispatcherReceiverCommand(val receiverCommand: ReceiverCommand) :
  EventDispatcherGenericCommand(type = CommandType.RECEIVER_COMMAND) {

  /** Enumeration of all possible actions for event receivers */
  enum class ReceiverCommand {
    START,
    STOP
  }
}
