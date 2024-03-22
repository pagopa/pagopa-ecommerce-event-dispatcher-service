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
      "http://localhost/key1/redirectionUrl",
      "key2",
      "http://localhost/key2/redirectionUrl",
      "key3",
      "http://localhost/key3/redirectionUrl")

  @ParameterizedTest
  @ValueSource(strings = ["key1", "key2", "key3"])
  fun shouldBuildPspBackendUriMapSuccessfully(pspId: String) {
    val mapping: Map<String, URI> = assertDoesNotThrow {
      checkoutRedirectConfigurationBuilder.redirectBeApiCallUriMap(pspToHandle, pspUriMap)
    }
    assertEquals("http://localhost/%s/redirectionUrl".formatted(pspId), mapping[pspId].toString())
  }

  @Test
  fun shouldThrowExceptionBuildingBackendUriMapForMissingApiKey() {
    val missingKeyPspMap: MutableMap<String, String> = HashMap(pspUriMap)
    missingKeyPspMap.remove("key1")
    val e: RedirectConfigurationException =
      assertThrows(RedirectConfigurationException::class.java) {
        checkoutRedirectConfigurationBuilder.redirectBeApiCallUriMap(pspToHandle, missingKeyPspMap)
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
      checkoutRedirectConfigurationBuilder.redirectBeApiCallUriMap(pspToHandle, missingKeyPspMap)
    }
  }
}
