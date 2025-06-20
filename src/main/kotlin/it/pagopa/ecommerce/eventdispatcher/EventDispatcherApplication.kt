package it.pagopa.ecommerce.eventdispatcher

import it.pagopa.ecommerce.commons.ConfigScan
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication
@EnableIntegration
@Import(ConfigScan::class)
class EventDispatcherApplication

fun main(args: Array<String>) {
  runApplication<EventDispatcherApplication>(*args)
}
