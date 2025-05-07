package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.domain.v2.Email
import it.pagopa.ecommerce.commons.utils.ConfidentialDataManager
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import java.util.function.Function
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import reactor.core.publisher.Mono

@OptIn(ExperimentalCoroutinesApi::class)
class ConfidentialDataUtilsTest {

  private val confidentialDataManager: ConfidentialDataManager = mock()
  private val walletSessionConfidentialDataManager: ConfidentialDataManager = mock()

  private val confidentialDataUtils =
    ConfidentialDataUtils(confidentialDataManager, walletSessionConfidentialDataManager)

  @Test
  fun `Should decrypt email correctly`() = runTest {

    /*
     * Prerequisite
     */
    given(confidentialDataManager.decrypt(any(), any<Function<String, Email>>()))
      .willReturn(Mono.just(Email(TransactionTestUtils.EMAIL_STRING)))
    /*
     * Test
     */
    val email = confidentialDataUtils.toEmail(TransactionTestUtils.EMAIL)
    /*
     * Assertions
     */
    assertEquals(TransactionTestUtils.EMAIL_STRING, email.value)
  }

  @Test
  fun `Should decrypt email in upper case correctly`() = runTest {
    val uppercaseMail = TransactionTestUtils.EMAIL_STRING.uppercase()
    /*
     * Prerequisite
     */
    given(confidentialDataManager.decrypt(any(), any<Function<String, Email>>()))
      .willReturn(Mono.just(Email(uppercaseMail)))
    /*
     * Test
     */
    val email = confidentialDataUtils.toEmail(TransactionTestUtils.EMAIL)
    /*
     * Assertions
     */
    assertEquals(uppercaseMail, email.value)
  }

  @Test
  fun `Should throw exception when an error occurs decrypting email`() = runTest {

    /*
     * Prerequisite
     */
    given(confidentialDataManager.decrypt(any(), any<Function<String, Email>>()))
      .willReturn(Mono.error(RuntimeException("Error decrypting email")))
    /*
     * Test
     */
    assertThrows<RuntimeException> { confidentialDataUtils.toEmail(TransactionTestUtils.EMAIL) }
  }
}
