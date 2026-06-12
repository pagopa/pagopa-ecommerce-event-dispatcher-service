package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.utils.RedirectUrlMappingConf
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
   * @param expectedMatchingCriteria
   * - search criteria that are expected to be found into loaded map. if some is not present an
   * error will be raised that will prevent module from starting up
   * @param pspUrlMapping
   * - configuration parameter that contains PSP to URI mapping
   * @return a configuration map for every PSPs
   */
  @Bean
  fun redirectUrlMappingConf(
    @Value("\${redirect.pspUrlMapping}") pspUrlMapping: String,
    @Value("\${redirect.expectedMatchingCriteria}") expectedMatchingCriteria: String
  ): RedirectUrlMappingConf {
    return RedirectUrlMappingConf(pspUrlMapping, expectedMatchingCriteria)
  }
}
