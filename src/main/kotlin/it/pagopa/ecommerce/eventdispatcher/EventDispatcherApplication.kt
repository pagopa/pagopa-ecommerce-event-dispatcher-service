package it.pagopa.ecommerce.eventdispatcher

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication
@EnableIntegration
class EventDispatcherApplication(private val applicationContext: ApplicationContext) :
  CommandLineRunner {
  override fun run(vararg args: String?) {
    applicationContext.beanDefinitionNames.forEach { beanName ->
      applicationContext.getBean(beanName)
    }
  }
}

fun main(args: Array<String>) {
  runApplication<EventDispatcherApplication>(*args)
}
