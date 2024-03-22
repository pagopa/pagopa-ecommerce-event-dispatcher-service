package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.exceptions.RedirectConfigurationException
import it.pagopa.ecommerce.commons.exceptions.RedirectConfigurationType
import java.net.URI
import java.util.function.Predicate
import java.util.stream.Collectors
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
class RedirectConfigurationsBuilder {
  /**
   * Create a {@code Map<String,URI>} that will associate, to every handled PSP, the backend URI to
   * be used to perform Redirect payment flow api call
   *
   * @param paymentTypeCodeList
   * - set of all redirect payment type codes to be handled flow
   * @param pspUrlMapping
   * - configuration parameter that contains PSP to URI mapping
   * @return a configuration map for every PSPs
   */
  @Bean
  fun redirectBeApiCallUriMap(
    @Value("\${redirect.paymentTypeCodeList}") paymentTypeCodeList: Set<String>,
    @Value("#{\${redirect.pspUrlMapping}}") pspUrlMapping: Map<String, String>
  ): Map<String, URI> {
    // URI.create throws IllegalArgumentException that will prevent module load for
    // invalid PSP URI configuration
    val redirectUriMap = pspUrlMapping.mapValues { URI.create(it.value) }
    val missingKeys =
      paymentTypeCodeList
        .stream()
        .filter(Predicate.not { key: String -> redirectUriMap.containsKey(key) })
        .collect(Collectors.toSet())
    if (missingKeys.isNotEmpty()) {
      throw RedirectConfigurationException(
        "Misconfigured redirect.pspUrlMapping, the following redirect payment type code b.e. URIs are not configured: %s".format(
          missingKeys),
        RedirectConfigurationType.BACKEND_URLS)
    }
    return redirectUriMap
  }
}
