package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.util.*

@Component
class RefundService(
    @Autowired private var paymentGatewayClient: PaymentGatewayClient,
    @Autowired private var npgClient: NpgClient,
    @Autowired @Qualifier("npgCardsApiKeys") private var npgCardsPspApiKey: Map<String, String>
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun requestNpgRefund(
        operationId: String,
        idempotenceKey: UUID,
        amount: BigDecimal,
        pspId: String
    ): Mono<RefundResponseDto> {
        return npgClient
            .refundPayment(
                UUID.randomUUID(),
                operationId,
                idempotenceKey,
                amount,
                npgCardsPspApiKey.getValue(pspId),
                "Refund request for transactionId $idempotenceKey and operationId $operationId"
            )
            .onErrorMap(NpgResponseException::class.java) { exception: NpgResponseException ->
                val throwable = exception.cause
                if (throwable is WebClientResponseException) {
                    val errorCodeReason = "Received error code from NPG: ${throwable.statusCode}"
                    if (throwable.statusCode.is5xxServerError) {
                        BadGatewayException(errorCodeReason)
                    } else {
                        RefundNotAllowedException(idempotenceKey, errorCodeReason)
                    }
                } else {
                    RefundNotAllowedException(idempotenceKey)
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
