package it.pagopa.ecommerce.eventdispatcher.redis.streams.commands

import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "_class", visible = false)
abstract class EventDispatcherCommandMixin :
  EventDispatcherGenericCommand(type = CommandType.RECEIVER_COMMAND) {}
