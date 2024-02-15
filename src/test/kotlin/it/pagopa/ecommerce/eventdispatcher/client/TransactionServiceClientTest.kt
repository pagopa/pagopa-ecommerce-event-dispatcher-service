package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.utils.getMockedTransactionInfoDto
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedUpdateAuthorizationReqeustDto
import it.pagopa.generated.transactionauthrequests.v1.api.TransactionsApi
import java.util.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class TransactionServiceClientTest {

  @Mock private lateinit var transactionsApi: TransactionsApi

  @InjectMocks private lateinit var transactionsServiceClient: TransactionsServiceClient

  @Test
  fun patchAuthRequest_200() {
    val testUIID: UUID = UUID.randomUUID()

    // preconditions
    Mockito.`when`(transactionsApi.updateTransactionAuthorization(eq(testUIID.toString()), any()))
      .thenReturn(Mono.just(getMockedTransactionInfoDto(testUIID)))

    // test
    val response =
      transactionsServiceClient
        .patchAuthRequest(testUIID, getMockedUpdateAuthorizationReqeustDto())
        .block()

    // asserts
    assertEquals("AUTHORIZATION_COMPLETED", response?.status?.value)
  }
  /*
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
  }*/
}
