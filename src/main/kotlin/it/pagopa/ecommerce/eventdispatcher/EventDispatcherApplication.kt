package it.pagopa.ecommerce.eventdispatcher

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication(scanBasePackages = ["it.pagopa.ecommerce"])
@EnableIntegration
class EventDispatcherApplication

fun main(args: Array<String>) {
  runApplication<EventDispatcherApplication>(*args)
}
