package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedNpgRefundResponse
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedVPosRefundRequest
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedXPayRefundRequest
import java.math.BigDecimal
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono

@TestPropertySource(locations = ["classpath:application.test.properties"])
class RefundServiceTest {
  private val paymentGatewayClient: PaymentGatewayClient = mock()
  private val npgClient: NpgClient = mock()
  private val apiKey = "mocked-api-key"
  private val refundService: RefundService = RefundService(paymentGatewayClient, npgClient, apiKey)

  @Test
  fun requestRefund_200_npg() {
    val operationId = "operationID"
    val idempotenceKey = "idempotenceKey"
    val amount = BigDecimal.valueOf(1000)

    // Precondition
    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any()))
      .thenReturn(Mono.just(getMockedNpgRefundResponse(operationId)))

    // Test
    val response = refundService.requestNpgRefund(operationId, idempotenceKey, amount).block()

    // Assertions
    assertEquals(operationId, response?.operationId)
    verify(npgClient, times(1))
      .refundPayment(any(), eq(operationId), eq(idempotenceKey), eq(amount), eq(apiKey))

    Mockito.`when`(npgClient.refundPayment(any(), any(), any(), any(), any())).thenCallRealMethod()
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
