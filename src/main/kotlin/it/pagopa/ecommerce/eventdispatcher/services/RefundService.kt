package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.math.BigDecimal
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class RefundService(
  @Autowired private var paymentGatewayClient: PaymentGatewayClient,
  @Autowired private var npgClient: NpgClient,
  @Autowired private var npgCardsPspApiKey: NpgPspApiKeysConfig
) {

  fun requestNpgRefund(
    operationId: String,
    idempotenceKey: UUID,
    amount: BigDecimal,
    pspId: String
  ): Mono<RefundResponseDto> {
    return npgCardsPspApiKey[pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        npgClient
          .refundPayment(
            UUID.randomUUID(),
            operationId,
            idempotenceKey,
            amount,
            apiKey,
            "Refund request for transactionId $idempotenceKey and operationId $operationId")
          .onErrorMap(NpgResponseException::class.java) { exception: NpgResponseException ->
            val responseStatusCode = exception.statusCode
            responseStatusCode
              .map {
                val errorCodeReason = "Received HTTP error code from NPG: $it"
                if (it.is5xxServerError) {
                  BadGatewayException(errorCodeReason)
                } else {
                  RefundNotAllowedException(idempotenceKey, errorCodeReason)
                }
              }
              .orElse(RefundNotAllowedException(idempotenceKey, "Unknown NPG HTTP response code"))
          }
      })
  }

  fun requestVposRefund(requestID: String): Mono<VposDeleteResponseDto> {
    return paymentGatewayClient.requestVPosRefund(UUID.fromString(requestID))
  }

  fun requestXpayRefund(requestID: String): Mono<XPayRefundResponse200Dto> {
    return paymentGatewayClient.requestXPayRefund(UUID.fromString(requestID))
  }
}
