package it.pagopa.ecommerce.eventdispatcher.config

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.config.ExpressionControlBusFactoryBean

@Configuration
class ControlBusConfiguration {

  @Bean @Qualifier("controlBusInCH") fun controlBusInputChannel() = DirectChannel()

  @Bean
  @Qualifier("controlBusOutCH")
  fun controlBusOutputChannel(): QueueChannel {
    return QueueChannel(10)
  }

  @Bean
  @ServiceActivator(inputChannel = "controlBusInputChannel")
  fun controlBus(): ExpressionControlBusFactoryBean {
    val controlBusFactoryBean = ExpressionControlBusFactoryBean()
    controlBusFactoryBean.setOutputChannel(controlBusOutputChannel())
    return controlBusFactoryBean
  }
}
