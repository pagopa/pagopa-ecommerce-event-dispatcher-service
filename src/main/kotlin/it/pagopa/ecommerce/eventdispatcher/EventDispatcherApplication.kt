package it.pagopa.ecommerce.eventdispatcher

import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication
@EnableIntegration
class EventDispatcherApplication(private val applicationContext: ApplicationContext) :
  CommandLineRunner {
  private val logger = LoggerFactory.getLogger(EventDispatcherApplication::class.java)
  override fun run(vararg args: String?) {
    logger.info("START performing bean warmup")
    applicationContext.beanDefinitionNames.forEach { beanName ->
      applicationContext.getBean(beanName)
    }
    logger.info("END performing bean warmup")
  }
}

fun main(args: Array<String>) {
  runApplication<EventDispatcherApplication>(*args)
}
