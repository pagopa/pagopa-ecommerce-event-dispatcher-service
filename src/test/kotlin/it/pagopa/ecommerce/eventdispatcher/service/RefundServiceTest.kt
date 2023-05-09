package it.pagopa.ecommerce.eventdispatcher.service

import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.utils.getMockedVPosRefundRequest
import java.util.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class RefundServiceTest {
  @Mock private lateinit var paymentGatewayClient: PaymentGatewayClient

  @InjectMocks private lateinit var refundService: RefundService

  @Test
  fun requestRefund_200() {
    val testUUID: UUID = UUID.randomUUID()

    // Precondition
    Mockito.`when`(paymentGatewayClient.requestVPosRefund(testUUID))
      .thenReturn(Mono.just(getMockedVPosRefundRequest(testUUID.toString())))

    // Test
    val response = refundService.requestVposRefund(testUUID.toString()).block()

    // Assertions
    assertEquals("CANCELLED", response?.status?.value)
  }
}
