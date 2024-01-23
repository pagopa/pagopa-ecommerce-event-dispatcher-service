package it.pagopa.ecommerce.eventdispatcher.redis.streams.commands

data class EventDispatcherReceiverCommand(val receiverCommand: ReceiverCommand) :
  EventDispatcherGenericCommand(type = CommandType.RECEIVER_COMMAND) {

  enum class ReceiverCommand {
    START,
    STOP
  }
}
