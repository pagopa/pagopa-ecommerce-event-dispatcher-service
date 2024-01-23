package it.pagopa.ecommerce.eventdispatcher

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.integration.config.EnableIntegration
import reactor.core.publisher.Hooks

@SpringBootApplication @EnableIntegration class EventDispatcherApplication

fun main(args: Array<String>) {
  Hooks.onOperatorDebug()
  runApplication<EventDispatcherApplication>(*args)
}
