package it.pagopa.ecommerce.eventdispatcher.redis.streams.commands

import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.util.*

/** Event dispatcher generic command event class */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "_class", visible = false)
sealed class EventDispatcherGenericCommand(
  val commandId: UUID = UUID.randomUUID(),
  val type: CommandType
) {
  enum class CommandType {
    RECEIVER_COMMAND
  }
}

class FakeUnmanagedCommand :
  EventDispatcherGenericCommand(type = EventDispatcherGenericCommand.CommandType.RECEIVER_COMMAND)
