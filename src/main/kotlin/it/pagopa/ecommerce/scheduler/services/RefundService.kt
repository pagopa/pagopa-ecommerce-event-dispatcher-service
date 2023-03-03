package it.pagopa.ecommerce.scheduler.services

import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class RefundService {
  @Autowired private lateinit var paymentGatewayClient: PaymentGatewayClient

  fun requestRefund(requestID: String): Mono<PostePayRefundResponseDto> {
    return paymentGatewayClient.requestRefund(UUID.fromString(requestID))
  }
}
