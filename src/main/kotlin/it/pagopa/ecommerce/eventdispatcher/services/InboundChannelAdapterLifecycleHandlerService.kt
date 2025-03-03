package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiverStatus
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.Status
import it.pagopa.ecommerce.eventdispatcher.redis.streams.RedisStreamConsumer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.ApplicationContext
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.QueueChannel
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Service

/**
 * This class handles InboundChannelAdapter lifecycle through SpEL ControlBus implementation. It
 * excludes the Redis stream components which are now handled via reactive approach.
 */
@Service
class InboundChannelAdapterLifecycleHandlerService(
  @Autowired private val applicationContext: ApplicationContext,
  @Autowired @Qualifier("controlBusInCH") private val controlBusInput: DirectChannel,
  @Autowired @Qualifier("controlBusOutCH") private val controlBusOutput: QueueChannel
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  /**
   * Invoke input command for all endpoints sending a message to the SpEL control bus input channel.
   * Excludes Redis stream components which are now handled via reactive approach.
   */
  fun invokeCommandForAllEndpoints(command: String) {
    logger.info("Invoking command [{}] for all eligible endpoints", command)
    findInboundChannelAdapterBeans()
      .filterNot { isBeanNameRedisRelated(it) }
      .forEach {
        logger.debug("Sending command [{}] to endpoint [{}]", command, it)
        val controllerBusMessage =
          MessageBuilder.createMessage("@${it}Endpoint.$command()", MessageHeaders(mapOf()))
        controlBusInput.send(controllerBusMessage)
      }
  }

  /**
   * Return all channels status querying the isRunning() method result sending a message to the SpEL
   * control bus input channel. Excludes Redis stream components.
   */
  fun getAllChannelStatus() =
    findInboundChannelAdapterBeans()
      .filterNot { isBeanNameRedisRelated(it) }
      .map { getChannelStatus(it) }

  /**
   * Retrieve channel status querying the isRunning() method result sending a message to the SpEL
   * control bus input channel
   */
  fun getChannelStatus(channelName: String): ReceiverStatus {
    val controllerBusMessage =
      MessageBuilder.createMessage("@${channelName}Endpoint.isRunning()", MessageHeaders(mapOf()))
    controlBusInput.send(controllerBusMessage)
    val responseMessage = controlBusOutput.receive(1000)
    val status =
      if (responseMessage?.payload is Boolean) {
        if (responseMessage.payload == true) {
          Status.UP
        } else {
          Status.DOWN
        }
      } else {
        Status.UNKNOWN
      }
    return ReceiverStatus(name = channelName, status = status)
  }

  /** Retrieve all InboundChannelAdapter on which perform commands */
  fun findInboundChannelAdapterBeans() =
    applicationContext.getBeansWithAnnotation(InboundChannelAdapter::class.java).keys

  /**
   * Determine if a bean name is related to Redis stream components. These are now handled via
   * reactive approach and should be excluded.
   */
  private fun isBeanNameRedisRelated(beanName: String): Boolean {
    return beanName.lowercase().contains("redis") ||
      applicationContext.getBeanNamesForType(RedisStreamConsumer::class.java).contains(beanName)
  }
}
