package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.domain.Email
import it.pagopa.ecommerce.commons.utils.ConfidentialDataManager
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
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
class ConfidentialMailUtilsTest {

  private val confidentialDataManager: ConfidentialDataManager = mock()

  private val confidentialMailUtils = ConfidentialMailUtils(confidentialDataManager)

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
    val email = confidentialMailUtils.toEmail(TransactionTestUtils.EMAIL)
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
    val email = confidentialMailUtils.toEmail(TransactionTestUtils.EMAIL)
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
    assertThrows<RuntimeException> { confidentialMailUtils.toEmail(TransactionTestUtils.EMAIL) }
  }
}
