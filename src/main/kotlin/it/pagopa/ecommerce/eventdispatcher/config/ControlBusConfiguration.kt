package it.pagopa.ecommerce.eventdispatcher.config

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.config.ControlBusFactoryBean

/**
 * ControlBus specific configurations. This class takes into account configurations for I/O channels
 * and SpEL expression message handler
 */
@Configuration
class ControlBusConfiguration {
  /** Inbound channel, used as input for SpEL expression control bus handler */
  @Bean @Qualifier("controlBusInCH") fun controlBusInputChannel() = DirectChannel()

  /** Outbound channel, used to retrieve SpEL expression control bus handler responses */
  @Bean
  @Qualifier("controlBusOutCH")
  fun controlBusOutputChannel(): QueueChannel {
    return QueueChannel(10)
  }

  /** Control bus handler that maps I/O channels to SpEL expression handler */
  @Bean
  @ServiceActivator(inputChannel = "controlBusInputChannel")
  fun controlBus(): ControlBusFactoryBean {
    val controlBusFactoryBean = ControlBusFactoryBean()
    controlBusFactoryBean.setOutputChannel(controlBusOutputChannel())
    return controlBusFactoryBean
  }
}
