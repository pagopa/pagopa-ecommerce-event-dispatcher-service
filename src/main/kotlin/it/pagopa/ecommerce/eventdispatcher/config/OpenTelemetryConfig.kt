package it.pagopa.ecommerce.eventdispatcher.config

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.utils.OpenTelemetryUtils
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenTelemetryConfig {
  @Bean fun agentOpenTelemetrySDKInstance(): OpenTelemetry = GlobalOpenTelemetry.get()

  @Bean
  fun openTelemetryTracer(openTelemetry: OpenTelemetry): Tracer =
    openTelemetry.getTracer("pagopa-ecommerce-event-dispatcher-service")

  @Bean
  fun tracingUtils(openTelemetry: OpenTelemetry, tracer: Tracer): TracingUtils =
    TracingUtils(openTelemetry, tracer)

  @Bean
  fun updateTransactionStatusTracerUtils(
    openTelemetryTracer: Tracer
  ): UpdateTransactionStatusTracerUtils =
    UpdateTransactionStatusTracerUtils(OpenTelemetryUtils(openTelemetryTracer))

  @Bean fun openTelemetryUtils(tracer: Tracer): OpenTelemetryUtils = OpenTelemetryUtils(tracer)
}
