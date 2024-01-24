package it.pagopa.ecommerce.eventdispatcher.redis.streams.commands

import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * Mixin class used for polymorphic event resolution using _class field during Redis Stream events
 * deserialization
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "_class", visible = false)
abstract class EventDispatcherCommandMixin :
  EventDispatcherGenericCommand(type = CommandType.RECEIVER_COMMAND)
