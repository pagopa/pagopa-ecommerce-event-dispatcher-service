package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.math.BigDecimal
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class RefundService(
  @Autowired private var paymentGatewayClient: PaymentGatewayClient,
  @Autowired private var npgClient: NpgClient,
  @Value("\${npg.client.apiKey}") private val npgApiKey: String
) {

  fun requestNpgRefund(
    operationId: String,
    idempotenceKey: UUID,
    amount: BigDecimal
  ): Mono<RefundResponseDto> {
    return npgClient
      .refundPayment(
        UUID.randomUUID(),
        operationId,
        idempotenceKey,
        amount,
        npgApiKey,
        "Refund request for transactionId $idempotenceKey and operationId $operationId")
      .onErrorMap(WebClientResponseException::class.java) { exception: WebClientResponseException ->
        when (exception.statusCode) {
          HttpStatus.NOT_FOUND -> TransactionNotFound(idempotenceKey)
          HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
          HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
          HttpStatus.BAD_REQUEST -> RefundNotAllowedException(idempotenceKey)
          else -> exception
        }
      }
  }

  fun requestVposRefund(requestID: String): Mono<VposDeleteResponseDto> {
    return paymentGatewayClient.requestVPosRefund(UUID.fromString(requestID))
  }

  fun requestXpayRefund(requestID: String): Mono<XPayRefundResponse200Dto> {
    return paymentGatewayClient.requestXPayRefund(UUID.fromString(requestID))
  }
}
