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
        LogTracingUtils.AttributeKeys.CTX_TRANSACTION_ID,
        LogTracingUtils.AttributeKeys.CTX_EVENT_CODE,
        LogTracingUtils.AttributeKeys.CTX_EVENT_ID,
        LogTracingUtils.AttributeKeys.EVENT_ACTION,
      ))
    return MDCContextLifterConfiguration()
  }
}
