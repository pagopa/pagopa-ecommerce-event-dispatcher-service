package it.pagopa.ecommerce.scheduler

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.integration.config.EnableIntegration

@SpringBootApplication @EnableIntegration class SchedulerApplication

fun main(args: Array<String>) {
  runApplication<SchedulerApplication>(*args)
}
