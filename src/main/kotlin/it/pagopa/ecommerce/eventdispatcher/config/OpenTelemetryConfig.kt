package it.pagopa.ecommerce.eventdispatcher.config

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenTelemetryConfig {
  @Bean fun agentOpenTelemetrySDKInstance(): OpenTelemetry = GlobalOpenTelemetry.get()

  @Bean
  fun openTelemetryTracer(openTelemetry: OpenTelemetry): Tracer =
    openTelemetry.getTracer("pagopa-ecommerce-event-dispatcher-service")
}
