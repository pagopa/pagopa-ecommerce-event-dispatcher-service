package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedRefundRequest
import it.pagopa.generated.ecommerce.gateway.v1.api.PaymentTransactionsControllerApi
import java.nio.charset.Charset
import java.util.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.TestPropertySource
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class PaymentGatewayClientTest {

  @Mock private lateinit var paymentTransactionsControllerApi: PaymentTransactionsControllerApi

  @InjectMocks private lateinit var paymentGatewayClient: PaymentGatewayClient

  @Test
  fun pgsClient_requestRefund_200() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(paymentTransactionsControllerApi.refundRequest(testUIID))
      .thenReturn(Mono.just(getMockedRefundRequest(testUIID.toString())))

    // test
    val response = paymentGatewayClient.requestRefund(testUIID).block()

    // asserts
    assertEquals("success", response?.refundOutcome)
  }

  @Test
  fun pgsClient_requestRefund_404() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(paymentTransactionsControllerApi.refundRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            404, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) { paymentGatewayClient.requestRefund(testUIID).block() }

    assertInstanceOf(TransactionNotFound::class.java, exc)
  }

  @Test
  fun pgsClient_requestRefund_504() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(paymentTransactionsControllerApi.refundRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.GatewayTimeout.create(
            504, "Gateway timeout", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) { paymentGatewayClient.requestRefund(testUIID).block() }

    assertTrue(exc.message?.contains("GatewayTimeoutException") ?: false)
  }
  @Test
  fun pgsClient_requestRefund_500() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(paymentTransactionsControllerApi.refundRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.InternalServerError.create(
            500, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) { paymentGatewayClient.requestRefund(testUIID).block() }

    assertInstanceOf(BadGatewayException::class.java, exc)
  }
}
