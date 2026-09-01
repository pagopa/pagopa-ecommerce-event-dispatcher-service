package it.pagopa.ecommerce.eventdispatcher.config

import io.micrometer.context.ContextRegistry
import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import jakarta.annotation.PostConstruct
import org.slf4j.MDC
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Hooks

@Configuration
class MicrometerMdcConfiguration {
  private val contextBound =
    setOf(
      LogTracingUtils.AttributeKeys.EVENT_ACTION.key,
      LogTracingUtils.AttributeKeys.CTX_TRANSACTION_ID.key,
      LogTracingUtils.AttributeKeys.CTX_EVENT_CODE.key,
      LogTracingUtils.AttributeKeys.CTX_EVENT_ID.key,
    )

  @PostConstruct
  fun initMdcMicrometerRegistry() {
    Hooks.enableAutomaticContextPropagation()
    LogTracingUtils.AttributeKeys.entries
      .filter { contextBound.contains(it.key) }
      .forEach { entry ->
        ContextRegistry.getInstance()
          .registerThreadLocalAccessor(
            entry.key,
            { MDC.get(entry.key) },
            { value -> MDC.put(entry.key, value) },
            { MDC.remove(entry.key) })
      }
  }
}
