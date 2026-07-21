package it.pagopa.ecommerce.eventdispatcher.mdcutilities

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.MDC
import reactor.util.context.Context

class EventDispatcherTracingUtilsTest {

  @AfterEach
  fun cleanMdc() {
    MDC.clear()
  }

  // ──────────────────────────────────────────────
  // TracingEntry enum
  // ──────────────────────────────────────────────

  @Test
  fun `TracingEntry values expose correct keys`() {
    assertEquals(
      "ctx.transaction.id", EventDispatcherTracingUtils.TracingEntry.CTX_TRANSACTION_ID.key)
    assertEquals("ctx.event.code", EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_CODE.key)
    assertEquals("ctx.event.id", EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_ID.key)
    assertEquals("event.action", EventDispatcherTracingUtils.TracingEntry.EVENT_ACTION.key)
    assertEquals("event.outcome", EventDispatcherTracingUtils.TracingEntry.EVENT_OUTCOME.key)
    assertEquals("dependency", EventDispatcherTracingUtils.TracingEntry.DEPENDENCY.key)
    assertEquals("error.type", EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key)
    assertEquals("error.message", EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.key)
  }

  @Test
  fun `TracingEntry values expose correct defaultValues`() {
    assertEquals(
      "{transactionId-not-found}",
      EventDispatcherTracingUtils.TracingEntry.CTX_TRANSACTION_ID.defaultValue)
    assertEquals(
      "{eventCode-not-found}", EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_CODE.defaultValue)
    assertEquals(
      "{eventId-not-found}", EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_ID.defaultValue)
    assertEquals(
      "{eventAction-not-found}", EventDispatcherTracingUtils.TracingEntry.EVENT_ACTION.defaultValue)
    assertEquals(
      "{errorType-not-found}", EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.defaultValue)
    assertEquals(
      "{errorMessage-not-found}",
      EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.defaultValue)
  }

  @Test
  fun `TracingEntry contextBound flag is correct`() {
    assertTrue(EventDispatcherTracingUtils.TracingEntry.CTX_TRANSACTION_ID.isContextBound)
    assertTrue(EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_CODE.isContextBound)
    assertTrue(EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_ID.isContextBound)
    assertTrue(EventDispatcherTracingUtils.TracingEntry.EVENT_ACTION.isContextBound)
    assertFalse(EventDispatcherTracingUtils.TracingEntry.EVENT_OUTCOME.isContextBound)
    assertFalse(EventDispatcherTracingUtils.TracingEntry.DEPENDENCY.isContextBound)
    assertFalse(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.isContextBound)
    assertFalse(EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.isContextBound)
  }

  // ──────────────────────────────────────────────
  // enrichContextForDispatcherEvent
  // ──────────────────────────────────────────────

  @Test
  fun `enrichContextForDispatcherEvent sets all keys when values are non-null`() {
    val ctx =
      EventDispatcherTracingUtils.enrichContextForDispatcherEvent(
        "tx-123", "EVENT_CODE", "event-id-1", Context.empty())

    assertEquals("tx-123", ctx.get(EventDispatcherTracingUtils.TracingEntry.CTX_TRANSACTION_ID.key))
    assertEquals("EVENT_CODE", ctx.get(EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_CODE.key))
    assertEquals("event-id-1", ctx.get(EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_ID.key))
    assertEquals(
      "PROCESS_DISPATCHER_EVENT",
      ctx.get(EventDispatcherTracingUtils.TracingEntry.EVENT_ACTION.key))
  }

  @Test
  fun `enrichContextForDispatcherEvent uses default values when inputs are null`() {
    val ctx =
      EventDispatcherTracingUtils.enrichContextForDispatcherEvent(null, null, null, Context.empty())

    assertEquals(
      EventDispatcherTracingUtils.TracingEntry.CTX_TRANSACTION_ID.defaultValue,
      ctx.get(EventDispatcherTracingUtils.TracingEntry.CTX_TRANSACTION_ID.key))
    assertEquals(
      EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_CODE.defaultValue,
      ctx.get(EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_CODE.key))
    assertEquals(
      EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_ID.defaultValue,
      ctx.get(EventDispatcherTracingUtils.TracingEntry.CTX_EVENT_ID.key))
  }

  @Test
  fun `enrichContextForDispatcherEvent preserves existing context entries`() {
    val existing = Context.of("pre-existing-key", "pre-existing-value")
    val ctx =
      EventDispatcherTracingUtils.enrichContextForDispatcherEvent("tx-id", "code", "id", existing)
    assertEquals("pre-existing-value", ctx.get<String>("pre-existing-key"))
  }

  // ──────────────────────────────────────────────
  // withErrorMdc(Throwable, Runnable)
  // ──────────────────────────────────────────────

  @Test
  fun `withErrorMdc sets error type and message in MDC during block execution`() {
    val exception = RuntimeException("something went wrong")
    var capturedType: String? = null
    var capturedMessage: String? = null

    EventDispatcherTracingUtils.withErrorMdc(exception) {
      capturedType = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key)
      capturedMessage = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.key)
    }

    assertEquals("java.lang.RuntimeException", capturedType)
    assertEquals("something went wrong", capturedMessage)
  }

  @Test
  fun `withErrorMdc removes error keys from MDC after block`() {
    EventDispatcherTracingUtils.withErrorMdc(RuntimeException("msg")) {}

    assertNull(MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key))
    assertNull(MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.key))
  }

  @Test
  fun `withErrorMdc cleans up MDC even when block throws`() {
    assertThrows<IllegalStateException> {
      EventDispatcherTracingUtils.withErrorMdc(RuntimeException("err")) {
        throw IllegalStateException("block-boom")
      }
    }

    assertNull(MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key))
    assertNull(MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.key))
  }

  @Test
  fun `withErrorMdc uses default values when error is null`() {
    var capturedType: String? = null
    var capturedMessage: String? = null

    EventDispatcherTracingUtils.withErrorMdc(null) {
      capturedType = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key)
      capturedMessage = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.key)
    }

    assertEquals(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.defaultValue, capturedType)
    assertEquals(
      EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.defaultValue, capturedMessage)
  }

  @Test
  fun `withErrorMdc uses default error message when exception has null message`() {
    val exception = RuntimeException(null as String?)
    var capturedMessage: String? = null

    EventDispatcherTracingUtils.withErrorMdc(exception) {
      capturedMessage = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.key)
    }

    assertEquals(
      EventDispatcherTracingUtils.TracingEntry.ERROR_MESSAGE.defaultValue, capturedMessage)
  }

  // ──────────────────────────────────────────────
  // withErrorMdc(Throwable, Map, Runnable)
  // ──────────────────────────────────────────────

  @Test
  fun `withErrorMdc with attributes sets all MDC keys during block`() {
    val exception = IllegalArgumentException("bad arg")
    val attributes = mapOf("custom.key" to "custom-value", "numeric" to 42)
    var capturedCustom: String? = null
    var capturedNumeric: String? = null
    var capturedType: String? = null

    EventDispatcherTracingUtils.withErrorMdc(exception, attributes) {
      capturedCustom = MDC.get("custom.key")
      capturedNumeric = MDC.get("numeric")
      capturedType = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key)
    }

    assertEquals("custom-value", capturedCustom)
    assertEquals("42", capturedNumeric)
    assertEquals("java.lang.IllegalArgumentException", capturedType)
  }

  @Test
  fun `withErrorMdc with attributes removes all keys after block`() {
    val attributes = mapOf("extra.key" to "value")

    EventDispatcherTracingUtils.withErrorMdc(RuntimeException("e"), attributes) {}

    assertNull(MDC.get("extra.key"))
    assertNull(MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key))
  }

  @Test
  fun `withErrorMdc with attributes skips null values in attributes map`() {
    val attributes = mapOf("present" to "yes", "absent" to null)
    var capturedPresent: String? = "UNSET"
    var capturedAbsent: String? = "UNSET"

    EventDispatcherTracingUtils.withErrorMdc(RuntimeException("e"), attributes) {
      capturedPresent = MDC.get("present")
      capturedAbsent = MDC.get("absent")
    }

    assertEquals("yes", capturedPresent)
    assertNull(capturedAbsent)
  }

  @Test
  fun `withErrorMdc with null attributes map works like single-arg overload`() {
    var capturedType: String? = null

    EventDispatcherTracingUtils.withErrorMdc(RuntimeException("msg"), null as Map<String, Any>?) {
      capturedType = MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key)
    }

    assertEquals("java.lang.RuntimeException", capturedType)
  }

  @Test
  fun `withErrorMdc with attributes cleans up even when block throws`() {
    val attributes = mapOf("k" to "v")

    assertThrows<RuntimeException> {
      EventDispatcherTracingUtils.withErrorMdc(RuntimeException("err"), attributes) {
        throw RuntimeException("block-throws")
      }
    }

    assertNull(MDC.get("k"))
    assertNull(MDC.get(EventDispatcherTracingUtils.TracingEntry.ERROR_TYPE.key))
  }

  // ──────────────────────────────────────────────
  // withContextDetailsMdc(Map, Runnable)
  // ──────────────────────────────────────────────

  @Test
  fun `withContextDetailsMdc serializes details map to JSON under ctx-details key`() {
    val details = mapOf("transactionId" to "tx-abc", "status" to "CLOSED")
    var captured: String? = null

    EventDispatcherTracingUtils.withContextDetailsMdc(details) { captured = MDC.get("ctx.details") }

    assertNotNull(captured)
    assertTrue(captured!!.contains("transactionId"))
    assertTrue(captured.contains("tx-abc"))
    assertTrue(captured.contains("status"))
    assertTrue(captured.contains("CLOSED"))
  }

  @Test
  fun `withContextDetailsMdc removes ctx-details key after block`() {
    EventDispatcherTracingUtils.withContextDetailsMdc(mapOf("k" to "v")) {}

    assertNull(MDC.get("ctx.details"))
  }

  @Test
  fun `withContextDetailsMdc uses empty JSON object when details map is null`() {
    var captured: String? = null

    EventDispatcherTracingUtils.withContextDetailsMdc(null as Map<String, Any>?) {
      captured = MDC.get("ctx.details")
    }

    assertEquals("{}", captured)
  }

  @Test
  fun `withContextDetailsMdc uses empty JSON object when details map is empty`() {
    var captured: String? = null

    EventDispatcherTracingUtils.withContextDetailsMdc(emptyMap<String, Any>()) {
      captured = MDC.get("ctx.details")
    }

    assertEquals("{}", captured)
  }

  @Test
  fun `withContextDetailsMdc cleans up even when block throws`() {
    assertThrows<RuntimeException> {
      EventDispatcherTracingUtils.withContextDetailsMdc(mapOf("key" to "val")) {
        throw RuntimeException("boom")
      }
    }

    assertNull(MDC.get("ctx.details"))
  }

  // ──────────────────────────────────────────────
  // withContextDetailsMdc(Map, Map, Runnable)
  // ──────────────────────────────────────────────

  @Test
  fun `withContextDetailsMdc with attributes sets ctx-details and extra keys during block`() {
    val details = mapOf("transactionId" to "tx-xyz")
    val attributes = mapOf("event.outcome" to "OK")
    var capturedDetails: String? = null
    var capturedOutcome: String? = null

    EventDispatcherTracingUtils.withContextDetailsMdc(details, attributes) {
      capturedDetails = MDC.get("ctx.details")
      capturedOutcome = MDC.get("event.outcome")
    }

    assertNotNull(capturedDetails)
    assertTrue(capturedDetails!!.contains("tx-xyz"))
    assertEquals("OK", capturedOutcome)
  }

  @Test
  fun `withContextDetailsMdc with attributes removes all keys after block`() {
    EventDispatcherTracingUtils.withContextDetailsMdc(mapOf("a" to "1"), mapOf("b" to "2")) {}

    assertNull(MDC.get("ctx.details"))
    assertNull(MDC.get("b"))
  }

  @Test
  fun `withContextDetailsMdc with attributes skips null values in attributes map`() {
    val attributes = mapOf("present" to "yes", "absent" to null)
    var capturedPresent: String? = "UNSET"
    var capturedAbsent: String? = "UNSET"

    EventDispatcherTracingUtils.withContextDetailsMdc(emptyMap<String, Any>(), attributes) {
      capturedPresent = MDC.get("present")
      capturedAbsent = MDC.get("absent")
    }

    assertEquals("yes", capturedPresent)
    assertNull(capturedAbsent)
  }

  @Test
  fun `withContextDetailsMdc with null attributes works like single-arg overload`() {
    val details = mapOf("foo" to "bar")
    var capturedDetails: String? = null

    EventDispatcherTracingUtils.withContextDetailsMdc(details, null as Map<String, Any>?) {
      capturedDetails = MDC.get("ctx.details")
    }

    assertNotNull(capturedDetails)
    assertTrue(capturedDetails!!.contains("bar"))
  }

  @Test
  fun `withContextDetailsMdc with attributes cleans up even when block throws`() {
    val attributes = mapOf("extra" to "value")

    assertThrows<RuntimeException> {
      EventDispatcherTracingUtils.withContextDetailsMdc(mapOf("d" to "v"), attributes) {
        throw RuntimeException("block-throws")
      }
    }

    assertNull(MDC.get("ctx.details"))
    assertNull(MDC.get("extra"))
  }
}
