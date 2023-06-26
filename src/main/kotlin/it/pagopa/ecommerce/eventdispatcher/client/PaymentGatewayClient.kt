package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.gateway.v1.api.VposInternalApi
import it.pagopa.generated.ecommerce.gateway.v1.api.XPayInternalApi
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.util.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class PaymentGatewayClient {
  @Autowired @Qualifier("VposApiWebClient") private lateinit var vposApi: VposInternalApi

  @Autowired @Qualifier("XpayApiWebClient") private lateinit var xpayApi: XPayInternalApi

  private val logger = LoggerFactory.getLogger(PaymentGatewayClient::class.java)

  fun requestXPayRefund(requestId: UUID): Mono<XPayRefundResponse200Dto> {
    logger.info("Performing XPAY refund for authorization id: [$requestId]")
    return xpayApi.refundXpayRequest(requestId).onErrorMap(
      WebClientResponseException::class.java) { exception: WebClientResponseException ->
      when (exception.statusCode) {
        HttpStatus.NOT_FOUND -> TransactionNotFound(requestId)
        HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
        HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
        HttpStatus.CONFLICT -> RefundNotAllowedException("")
        else -> exception
      }
    }
  }

  fun requestVPosRefund(requestId: UUID): Mono<VposDeleteResponseDto> {
    logger.info("Performing VPOS refund for authorization id: [$requestId]")
    return vposApi.requestPaymentsVposRequestIdDelete(requestId.toString()).onErrorMap(
      WebClientResponseException::class.java) { exception: WebClientResponseException ->
      when (exception.statusCode) {
        HttpStatus.NOT_FOUND -> TransactionNotFound(requestId)
        HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
        HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
        HttpStatus.CONFLICT -> RefundNotAllowedException("")
        else -> exception
      }
    }
  }
}
