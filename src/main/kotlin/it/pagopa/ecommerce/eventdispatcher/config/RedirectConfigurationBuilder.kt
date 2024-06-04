package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.utils.RedirectKeysConfiguration
import lombok.extern.slf4j.Slf4j
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Configuration class used to read all the PSP configurations that will be used during redirect
 * transaction
 */
@Configuration
@Slf4j
class RedirectConfigurationBuilder {
  /**
   * Create a {@code RedirectKeysConfiguration} that will contain to every handled PSP, the backend
   * URI to be used to perform Redirect payment flow api call, then provides a method to search
   * based on the touchpoint key - pspId - paymentTypeCode keys
   *
   * @param paymentTypeCodes
   * - set of all redirect payment type codes to be handled flow
   * @param pspUrlMapping
   * - configuration parameter that contains PSP to URI mapping
   * @return a configuration map for every PSPs
   */
  @Bean
  fun redirectBeApiCallUriConf(
    @Value("#{\${redirect.pspUrlMapping}}") pspUrlMapping: Map<String, String>,
    @Value("\${redirect.paymentTypeCodes}") paymentTypeCodes: Set<String>
  ): RedirectKeysConfiguration {
    // URI.create throws IllegalArgumentException that will prevent module load for
    // invalid PSP URI configuration
    // the redirect url configuration map is in common and it's used to configure both redirections
    // and redirections/refunds endpoints. here we want to configure refunds endpoint only since
    // it's the only api call that will be performed by event dispatcher for redirections payment
    // flow
    val redirectUriMap = pspUrlMapping.mapValues { "${it.value}/refunds" }
    return RedirectKeysConfiguration(redirectUriMap, paymentTypeCodes)
  }
}
