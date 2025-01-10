package it.pagopa.ecommerce.eventdispatcher

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Lazy
import org.springframework.integration.config.EnableIntegration
import javax.annotation.PostConstruct

@SpringBootApplication
@EnableIntegration
class EventDispatcherApplication(private val applicationContext: ApplicationContext) {

  @PostConstruct
  fun initializeLazyBeans() {
    applicationContext.beanDefinitionNames.forEach { beanName ->
      applicationContext.getBean(beanName)
    }
  }
}

fun main(args: Array<String>) {
  runApplication<EventDispatcherApplication>(*args)
}