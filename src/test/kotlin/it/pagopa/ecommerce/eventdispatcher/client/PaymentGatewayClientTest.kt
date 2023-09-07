package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedVPosRefundRequest
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedXPayRefundRequest
import it.pagopa.generated.ecommerce.gateway.v1.api.VposInternalApi
import it.pagopa.generated.ecommerce.gateway.v1.api.XPayInternalApi
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

  @Mock private lateinit var vposApi: VposInternalApi
  @Mock private lateinit var xpayApi: XPayInternalApi
  @Mock private lateinit var npgClient: NpgClient

  @InjectMocks private lateinit var paymentGatewayClient: PaymentGatewayClient

  @Test
  fun pgsClient_requestRefund_200_xpay() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(Mono.just(getMockedXPayRefundRequest(testUIID.toString())))

    // test
    val response = paymentGatewayClient.requestXPayRefund(testUIID).block()

    // asserts
    assertEquals("CANCELLED", response?.status?.value)
  }

  @Test
  fun pgsClient_requestRefund_200_vpos() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(vposApi.requestPaymentsVposRequestIdDelete(testUIID.toString()))
      .thenReturn(Mono.just(getMockedVPosRefundRequest(testUIID.toString())))

    // test
    val response = paymentGatewayClient.requestVPosRefund(testUIID).block()

    // asserts
    assertEquals("CANCELLED", response?.status?.value)
  }

  @Test
  fun pgsClient_requestRefund_404_xpay() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            404, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestXPayRefund(testUIID).block()
      }

    assertInstanceOf(TransactionNotFound::class.java, exc)
  }

  @Test
  fun pgsClient_requestRefund_404_vpos() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(vposApi.requestPaymentsVposRequestIdDelete(testUIID.toString()))
      .thenReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            404, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestVPosRefund(testUIID).block()
      }

    assertInstanceOf(TransactionNotFound::class.java, exc)
  }

  @Test
  fun pgsClient_requestRefund_504_xpay() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.GatewayTimeout.create(
            504, "Gateway timeout", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestXPayRefund(testUIID).block()
      }

    assertTrue(exc.message?.contains("GatewayTimeoutException") ?: false)
  }

  @Test
  fun pgsClient_requestRefund_504_vpos() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(vposApi.requestPaymentsVposRequestIdDelete(testUIID.toString()))
      .thenReturn(
        Mono.error(
          WebClientResponseException.GatewayTimeout.create(
            504, "Gateway timeout", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestVPosRefund(testUIID).block()
      }

    assertTrue(exc.message?.contains("GatewayTimeoutException") ?: false)
  }

  @Test
  fun pgsClient_requestRefund_500_xpay() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.InternalServerError.create(
            500, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestXPayRefund(testUIID).block()
      }

    assertInstanceOf(BadGatewayException::class.java, exc)
  }

  @Test
  fun pgsClient_requestRefund_500_vpos() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(vposApi.requestPaymentsVposRequestIdDelete(testUIID.toString()))
      .thenReturn(
        Mono.error(
          WebClientResponseException.InternalServerError.create(
            500, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestVPosRefund(testUIID).block()
      }

    assertInstanceOf(BadGatewayException::class.java, exc)
  }

  @Test
  fun pgsClient_Exception_503_vpos() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(vposApi.requestPaymentsVposRequestIdDelete(testUIID.toString()))
      .thenReturn(
        Mono.error(
          WebClientResponseException.ServiceUnavailable.create(
            503, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestVPosRefund(testUIID).block()
      }

    assertInstanceOf(Exception::class.java, exc)
  }
  @Test
  fun pgsClient_Exception_503_xpay() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.ServiceUnavailable.create(
            503, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestXPayRefund(testUIID).block()
      }

    assertInstanceOf(Exception::class.java, exc)
  }

  @Test
  fun pgsClient_Exception_409_refund_xpay() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.Conflict.create(
            409, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestXPayRefund(testUIID).block()
      }

    assertInstanceOf(RefundNotAllowedException::class.java, exc)
  }

  @Test
  fun pgsClient_Exception_409_refund_vpos() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(xpayApi.refundXpayRequest(testUIID))
      .thenReturn(
        Mono.error(
          WebClientResponseException.Conflict.create(
            409, "", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // test
    val exc =
      assertThrows(Exception::class.java) {
        paymentGatewayClient.requestXPayRefund(testUIID).block()
      }

    assertInstanceOf(RefundNotAllowedException::class.java, exc)
  }
}
