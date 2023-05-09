package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class RefundService {
  @Autowired private lateinit var paymentGatewayClient: PaymentGatewayClient

  fun requestPostePayRefund(requestID: String): Mono<PostePayRefundResponseDto> {
    return paymentGatewayClient.requestPostepayRefund(UUID.fromString(requestID))
  }

  fun requestVposRefund(requestID: String): Mono<VposDeleteResponseDto> {
    return paymentGatewayClient.requestVPosRefund(UUID.fromString(requestID))
  }

  fun requestXpayRefund(requestID: String): Mono<XPayRefundResponse200Dto> {
    return paymentGatewayClient.requestXPayRefund(UUID.fromString(requestID))
  }
}
