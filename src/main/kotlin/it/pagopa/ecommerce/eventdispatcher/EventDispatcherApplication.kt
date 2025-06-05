package it.pagopa.ecommerce.eventdispatcher

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication
@ComponentScan(
  basePackages = ["it.pagopa.ecommerce.commons", "it.pagopa.ecommerce.eventdispatcher"])
@EnableIntegration
class EventDispatcherApplication

fun main(args: Array<String>) {
  runApplication<EventDispatcherApplication>(*args)
}
