package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.commons.mdcutilities.MDCContextLifterConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class LogTracingUtilsConfig {

  @Bean
  fun initializeMdcContextLifter(): MDCContextLifterConfiguration {
    LogTracingUtils.setContextBounded(
      setOf(
        LogTracingUtils.TracingEntry.CTX_TRANSACTION_ID,
        LogTracingUtils.TracingEntry.CTX_EVENT_CODE,
        LogTracingUtils.TracingEntry.CTX_EVENT_ID,
        LogTracingUtils.TracingEntry.EVENT_ACTION,
      ))
    return MDCContextLifterConfiguration()
  }
}
