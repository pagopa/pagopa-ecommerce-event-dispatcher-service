package it.pagopa.ecommerce.scheduler.client

import it.pagopa.ecommerce.scheduler.utils.TestUtil.Companion.getMockedRefundRequest
import it.pagopa.generated.ecommerce.gateway.v1.api.PaymentTransactionsControllerApi
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
class PaymentGatewayClientTest {

    @Mock
    private lateinit var paymentTransactionsControllerApi: PaymentTransactionsControllerApi

    @InjectMocks
    private lateinit var paymentGatewayClient: PaymentGatewayClient

    @Test
    fun pgsClient_requestRefund_200(){
        val testUIID: UUID = UUID.randomUUID()

        // preconditions
        Mockito.`when`(paymentTransactionsControllerApi.refundRequest(testUIID))
            .thenReturn(Mono.just(getMockedRefundRequest(testUIID.toString())))

        // test
        val response = paymentGatewayClient.requestRefund(testUIID).block()

        // asserts
        assertEquals("success", response?.refundOutcome)
    }
}