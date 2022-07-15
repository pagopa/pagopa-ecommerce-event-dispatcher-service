package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.services.RefundService
import it.pagopa.ecommerce.scheduler.utils.TestUtil.Companion.getMockedRefundRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Mono
import java.util.*

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
class RefundServiceTest {
    @Mock
    private lateinit var paymentGatewayClient: PaymentGatewayClient

    @InjectMocks
    private lateinit var refundService: RefundService

    @Test
    fun requestRefund_200(){
        val testUUID: UUID = UUID.randomUUID()

        // Precondition
        Mockito.`when`(paymentGatewayClient.requestRefund(testUUID))
            .thenReturn(Mono.just(getMockedRefundRequest(testUUID.toString())))

        // Test
        val response = refundService.requestRefund(testUUID.toString()).block()

        // Assertions
        assertEquals("success", response?.refundOutcome)
    }
}