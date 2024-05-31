package it.pagopa.ecommerce.eventdispatcher.config

import it.pagopa.ecommerce.commons.exceptions.RedirectConfigurationException
import java.net.URI
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class RedirectConfigurationBuilderTest {
  private val checkoutRedirectConfigurationBuilder = RedirectConfigurationBuilder()
  private val pspToHandle = setOf("key1", "key2", "key3")
  private val pspUriMap =
    java.util.Map.of(
      "key1",
      "http://localhost/key1/redirections",
      "key2",
      "http://localhost/key2/redirections",
      "key3",
      "http://localhost/key3/redirections")

  @ParameterizedTest
  @ValueSource(strings = ["key1", "key2", "key3"])
  fun shouldBuildPspBackendUriMapSuccessfully(pspId: String) {
    val mapping: Map<String, URI> = assertDoesNotThrow {
      checkoutRedirectConfigurationBuilder
        .redirectBeApiCallUriConf(pspUriMap, pspToHandle)
        .redirectBeApiCallUriMap
    }
    assertEquals(
      "http://localhost/%s/redirections/refunds".format(pspId), mapping[pspId].toString())
  }

  @Test
  fun shouldThrowExceptionBuildingBackendUriMapForMissingApiKey() {
    val missingKeyPspMap: MutableMap<String, String> = HashMap(pspUriMap)
    missingKeyPspMap.remove("key1")
    val e: RedirectConfigurationException =
      assertThrows(RedirectConfigurationException::class.java) {
        checkoutRedirectConfigurationBuilder.redirectBeApiCallUriConf(missingKeyPspMap, pspToHandle)
      }
    assertEquals(
      "Error parsing Redirect PSP BACKEND_URLS configuration, cause: Misconfigured redirect.pspUrlMapping, the following redirect payment type code b.e. URIs are not configured: [key1]",
      e.message)
  }

  @Test
  fun shouldThrowExceptionBuildingBackendUriMapForWrongUri() {
    val missingKeyPspMap: MutableMap<String, String> = HashMap(pspUriMap)
    missingKeyPspMap["key1"] = "http:\\\\localhost"
    assertThrows(IllegalArgumentException::class.java) {
      checkoutRedirectConfigurationBuilder.redirectBeApiCallUriConf(missingKeyPspMap, pspToHandle)
    }
  }
}
