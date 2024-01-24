package it.pagopa.ecommerce.eventdispatcher.config.redis.stream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherCommandMixin
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import java.util.*
import javax.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.connection.stream.*
import org.springframework.data.redis.stream.StreamReceiver
import org.springframework.integration.endpoint.AbstractMessageSource
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.GenericMessage
import org.springframework.stereotype.Component

/**
 * Redis Stream message source implementation. This class serves as message source for
 * InboundChannelAdapter integration: one event per time is retrieved from Redis Stream and acked
 * automatically. This class generate also a unique consumer group using input consumer group and
 * name prefixes adding a random string, generating a unique consumer group each time this class is
 * created. Finally this class takes into account also consumer group deletion before destroy, in
 * order to clear allocated consumer group. Each parsed Redis Stream event is packaged into a
 * Message where payload is the parsed event adding some custom headers to the message:
 * - REDIS_EVENT_ID: valued with the read Redis event unique event identifier
 * - REDIS_EVENT_TIMESTAMP: valued with the Redis event write timestamp
 * - REDIS_EVENT_STREAM_KEY: values with the Redis event stream key from which this event have been
 * retrieved
 */
@Component
class RedisStreamMessageSource(
  @Autowired
  private val redisStreamReceiver:
    StreamReceiver<String, ObjectRecord<String, LinkedHashMap<*, *>>>,
  @Autowired
  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper,
  @Value("\${redisStream.streamKey}") private val streamKey: String,
  @Value("\${redisStream.consumerGroupPrefix}") private val consumerGroupPrefix: String,
  @Value("\${redisStream.consumerNamePrefix}") private val consumerNamePrefix: String
) : AbstractMessageSource<Message<Any>>(), AutoCloseable {

  companion object {
    const val REDIS_EVENT_ID = "REDIS_EVENT_ID"
    const val REDIS_EVENT_TIMESTAMP = "REDIS_EVENT_TIMESTAMP"
    const val REDIS_EVENT_STREAM_KEY = "REDIS_EVENT_STREAM_KEY"
  }

  object RedisStreamMessageSourceLogger {
    val logger: Logger = LoggerFactory.getLogger(AbstractMessageSource::class.java)
  }

  private val uniqueConsumerId = UUID.randomUUID().toString()
  private val consumerGroup = "$consumerGroupPrefix-$uniqueConsumerId"
  private val consumerName = "$consumerNamePrefix-$uniqueConsumerId"

  private val objectMapper: ObjectMapper =
    jacksonObjectMapper()
      .addMixIn(EventDispatcherGenericCommand::class.java, EventDispatcherCommandMixin::class.java)

  init {
    eventDispatcherCommandsTemplateWrapper
      .unwrap()
      .opsForStream<String, String>()
      .createGroup(streamKey, consumerGroup)
  }

  override fun getComponentType(): String = "redis-stream:message-source"

  override fun doReceive(): Message<EventDispatcherGenericCommand>? {
    val message =
      runCatching {
          redisStreamReceiver
            .receiveAutoAck(
              Consumer.from(consumerGroup, consumerName),
              StreamOffset.create(streamKey, ReadOffset.lastConsumed()))
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
            .blockFirst()
        }
        .onFailure {
          RedisStreamMessageSourceLogger.logger.error("Exception receiving Redis Stream event", it)
        }
    return message.getOrNull()
  }

  @PreDestroy
  override fun close() {
    RedisStreamMessageSourceLogger.logger.info("Deleting consumer group with id $consumerGroup")
    eventDispatcherCommandsTemplateWrapper
      .unwrap()
      .opsForStream<String, String>()
      .destroyGroup(streamKey, consumerGroup)
  }
}
