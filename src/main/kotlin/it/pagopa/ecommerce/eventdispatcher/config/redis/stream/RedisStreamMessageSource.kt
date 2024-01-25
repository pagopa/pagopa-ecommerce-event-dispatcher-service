package it.pagopa.ecommerce.eventdispatcher.config.redis.stream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherCommandsTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherCommandMixin
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.stream.StreamReceiver
import org.springframework.integration.endpoint.AbstractMessageSource
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.GenericMessage

class RedisStreamMessageSource(
  private val redisStreamReceiver:
    StreamReceiver<String, ObjectRecord<String, LinkedHashMap<*, *>>>,
  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper,
  private val redisStreamConf: RedisStreamEventControllerConfigs
) : AbstractMessageSource<Message<Any>>() {

  companion object {
    const val REDIS_EVENT_ID = "REDIS_EVENT_ID"
    const val REDIS_EVENT_TIMESTAMP = "REDIS_EVENT_TIMESTAMP"
    const val REDIS_EVENT_STREAM_KEY = "REDIS_EVENT_STREAM_KEY"
  }

  object RedisStreamMessageSourceLogger {
    val logger: Logger = LoggerFactory.getLogger(AbstractMessageSource::class.java)
  }

  private val objectMapper: ObjectMapper =
    jacksonObjectMapper()
      .addMixIn(EventDispatcherGenericCommand::class.java, EventDispatcherCommandMixin::class.java)

  init {
    runCatching {
        eventDispatcherCommandsTemplateWrapper.createGroup(
          redisStreamConf.streamKey, redisStreamConf.consumerGroup)
      }
      .onFailure {
        RedisStreamMessageSourceLogger.logger.error("Error creating consumer group", it)
        if (redisStreamConf.faiOnErrorCreatingConsumerGroup) {
          throw IllegalStateException("Error creating consumer group for stream event receiver", it)
        }
      }
      .onSuccess {
        RedisStreamMessageSourceLogger.logger.info(
          "Consumer group created successfully for stream [${redisStreamConf.streamKey}] and group id: [${redisStreamConf.consumerGroup}]")
      }
  }

  override fun getComponentType(): String = "redis-stream:message-source"

  public override fun doReceive(): Message<EventDispatcherGenericCommand>? {
    val message =
      runCatching {
          redisStreamReceiver
            .receiveAutoAck(
              Consumer.from(redisStreamConf.consumerGroup, redisStreamConf.consumerName),
              StreamOffset.create(redisStreamConf.streamKey, ReadOffset.lastConsumed()))
            .take(1)
            .map {
              RedisStreamMessageSourceLogger.logger.debug(
                "Redis stream deserialization map: {}", it.value)
              val parsedEvent =
                objectMapper.convertValue(it.value, EventDispatcherGenericCommand::class.java)
              val messageHeaders =
                MessageHeaders(
                  mapOf(
                    REDIS_EVENT_ID to it.id.value,
                    REDIS_EVENT_TIMESTAMP to it.id.timestamp,
                    REDIS_EVENT_STREAM_KEY to it.stream))
              GenericMessage(parsedEvent, messageHeaders)
            }
            /*
             * block at most 500 millis to retrieve events from event stream.
             * this timeout is to be intended for a single stream read operation
             * and has been set in order to
             */
            .blockFirst(Duration.ofMillis(500))
        }
        .onFailure {
          /*
           * this exception is thrown when no event can be retrieved before timeout expiration
           * and has been skipped from logged exceptions since this will happen at each event retrieval
           * polling execution
           */
          if (it !is IllegalStateException) {
            RedisStreamMessageSourceLogger.logger.error(
              "Exception receiving Redis Stream event", it)
          }
        }
    return message.getOrNull()
  }
}
