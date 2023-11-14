package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedNpgRefundResponse
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedVPosRefundRequest
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedXPayRefundRequest
import java.math.BigDecimal
import java.nio.charset.Charset
import java.util.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.TestPropertySource
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class RefundServiceTests {
  private val paymentGatewayClient: PaymentGatewayClient = mock()
  private val npgClient: NpgClient = mock()
  private val pspId = "pspId1"
  private val npgPspApiKeys = mapOf("pspId1" to "pspKey1")
  private val refundService: RefundService =
    RefundService(paymentGatewayClient, npgClient, npgPspApiKeys)

  @Test
  fun requestRefund_200_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .willReturn(Mono.just(getMockedNpgRefundResponse(operationId)))

    // Test
    val response =
      refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId).block()

    // Assertions
    assertEquals(operationId, response!!.operationId)
    verify(npgClient, times(1))
      .refundPayment(
        any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(npgPspApiKeys[pspId]!!), any())

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
  }

  @Test
  fun requestRefund_400_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given { npgClient.refundPayment(any(), any(), any(), any(), any(), any()) }
      .willReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            400, "bad request", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // Test

    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(RefundNotAllowedException::class.java)
      .verify()

    verify(npgClient, times(1))
      .refundPayment(
        any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(npgPspApiKeys[pspId]!!), any())

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
  }

  @Test
  fun requestRefund_404_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given { npgClient.refundPayment(any(), any(), any(), any(), any(), any()) }
      .willReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            404, "not found", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // Test

    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(TransactionNotFound::class.java)
      .verify()

    verify(npgClient, times(1))
      .refundPayment(
        any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(npgPspApiKeys[pspId]!!), any())

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
  }

  @Test
  fun requestRefund_401_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given { npgClient.refundPayment(any(), any(), any(), any(), any(), any()) }
      .willReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            401, "unauthorized", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // Test

    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(WebClientResponseException::class.java)
      .verify()

    verify(npgClient, times(1))
      .refundPayment(
        any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(npgPspApiKeys[pspId]!!), any())

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
  }

  @Test
  fun requestRefund_500_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given { npgClient.refundPayment(any(), any(), any(), any(), any(), any()) }
      .willReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            500,
            "internal server errror",
            HttpHeaders.EMPTY,
            ByteArray(0),
            Charset.defaultCharset())))

    // Test

    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(BadGatewayException::class.java)
      .verify()

    verify(npgClient, times(1))
      .refundPayment(
        any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(npgPspApiKeys[pspId]!!), any())

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
  }

  @Test
  fun requestRefund_504_npg() {
    val operationId = "operationID"
    val idempotenceKey = UUID.randomUUID()
    val amount = BigDecimal.valueOf(1000)
    val pspId = "pspId1"
    // Precondition
    given { npgClient.refundPayment(any(), any(), any(), any(), any(), any()) }
      .willReturn(
        Mono.error(
          WebClientResponseException.BadRequest.create(
            504, "timeout", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset())))

    // Test

    StepVerifier.create(refundService.requestNpgRefund(operationId, idempotenceKey, amount, pspId))
      .expectError(GatewayTimeoutException::class.java)
      .verify()

    verify(npgClient, times(1))
      .refundPayment(
        any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(npgPspApiKeys[pspId]!!), any())

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
  }

  @Test
  fun requestRefund_200_vpos() {
    val testUUID: UUID = UUID.randomUUID()

    // Precondition
    Mockito.`when`(paymentGatewayClient.requestVPosRefund(testUUID))
      .thenReturn(Mono.just(getMockedVPosRefundRequest(testUUID.toString())))
    // Test
    val response = refundService.requestVposRefund(testUUID.toString()).block()
    // Assertions
    assertEquals("CANCELLED", response?.status?.value)
  }

  @Test
  fun requestRefund_200_xpay() {
    val testUUID: UUID = UUID.randomUUID()

    // Precondition
    Mockito.`when`(paymentGatewayClient.requestXPayRefund(testUUID))
      .thenReturn(Mono.just(getMockedXPayRefundRequest(testUUID.toString())))

    // Test
    val response = refundService.requestXpayRefund(testUUID.toString()).block()

    // Assertions
    assertEquals("CANCELLED", response?.status?.value)
  }
}
