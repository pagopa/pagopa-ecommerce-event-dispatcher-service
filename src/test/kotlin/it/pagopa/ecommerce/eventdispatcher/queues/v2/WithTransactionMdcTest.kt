package it.pagopa.ecommerce.eventdispatcher.queues.v2

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.MDC

class WithTransactionMdcTest {

  @Test
  fun `simple overload should set CTX_TRANSACTION_ID during block and remove it after`() {
    val txId = "abc123"
    var mdcDuringBlock: String? = null

    withTransactionMdc(txId) { mdcDuringBlock = MDC.get(CTX_TRANSACTION_ID) }

    assertEquals(txId, mdcDuringBlock)
    assertNull(MDC.get(CTX_TRANSACTION_ID), "CTX_TRANSACTION_ID should be removed after block")
  }

  @Test
  fun `simple overload should return the block result`() {
    val result = withTransactionMdc("tx-id") { 42 }
    assertEquals(42, result)
  }

  @Test
  fun `details overload should set all MDC keys during block and remove them after`() {
    val txId = "tx-details"
    val details = mapOf("key1" to "value1", "key2" to 99)
    var mdcTxId: String? = null
    var mdcKey1: String? = null
    var mdcKey2: String? = null

    withTransactionMdc(txId, details) {
      mdcTxId = MDC.get(CTX_TRANSACTION_ID)
      mdcKey1 = MDC.get("${CTX_DETAILS_PREFIX}key1")
      mdcKey2 = MDC.get("${CTX_DETAILS_PREFIX}key2")
    }

    assertEquals(txId, mdcTxId)
    assertEquals("value1", mdcKey1)
    assertEquals("99", mdcKey2)
    assertNull(MDC.get(CTX_TRANSACTION_ID), "CTX_TRANSACTION_ID should be removed after block")
    assertNull(MDC.get("${CTX_DETAILS_PREFIX}key1"), "detail key1 should be removed after block")
    assertNull(MDC.get("${CTX_DETAILS_PREFIX}key2"), "detail key2 should be removed after block")
  }

  @Test
  fun `details overload should filter out null values and not put them in MDC`() {
    val txId = "tx-null-details"
    val details: Map<String, Any?> = mapOf("present" to "yes", "absent" to null)
    var mdcPresent: String? = "UNSET"
    var mdcAbsent: String? = "UNSET"

    withTransactionMdc(txId, details) {
      mdcPresent = MDC.get("${CTX_DETAILS_PREFIX}present")
      mdcAbsent = MDC.get("${CTX_DETAILS_PREFIX}absent")
    }

    assertEquals("yes", mdcPresent)
    assertNull(mdcAbsent, "null-valued detail should NOT be added to MDC")
    assertNull(
      MDC.get("${CTX_DETAILS_PREFIX}present"), "present detail key should be removed after block")
  }

  @Test
  fun `MDC keys should be cleaned up in the finally block even when the block throws`() {
    val txId = "tx-throw"
    val details: Map<String, Any?> = mapOf("k" to "v")

    assertThrows<RuntimeException> {
      withTransactionMdc(txId, details) { throw RuntimeException("boom") }
    }

    assertNull(MDC.get(CTX_TRANSACTION_ID), "CTX_TRANSACTION_ID must be removed on exception")
    assertNull(MDC.get("${CTX_DETAILS_PREFIX}k"), "detail key must be removed on exception")
  }

  @Test
  fun `details overload with empty map should behave like simple overload`() {
    val txId = "tx-empty"
    var mdcDuringBlock: String? = null

    withTransactionMdc(txId, emptyMap()) { mdcDuringBlock = MDC.get(CTX_TRANSACTION_ID) }

    assertEquals(txId, mdcDuringBlock)
    assertNull(MDC.get(CTX_TRANSACTION_ID))
  }
}
