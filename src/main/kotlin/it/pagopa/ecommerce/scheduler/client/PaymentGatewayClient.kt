package it.pagopa.ecommerce.scheduler.client

import it.pagopa.ecommerce.scheduler.exceptions.BadGatewayException
import it.pagopa.ecommerce.scheduler.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.scheduler.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.gateway.v1.api.PaymentTransactionsControllerApi
import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class PaymentGatewayClient {
  @Autowired
  @Qualifier("paymentTransactionGatewayWebClient")
  private lateinit var paymentTransactionsControllerApi: PaymentTransactionsControllerApi

  fun requestRefund(requestId: UUID): Mono<PostePayRefundResponseDto> {
    return paymentTransactionsControllerApi.refundRequest(requestId).onErrorMap(
      WebClientResponseException::class.java) { exception: WebClientResponseException ->
      when (exception.statusCode) {
        HttpStatus.NOT_FOUND -> TransactionNotFound(requestId)
        HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
        HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
        else -> exception
      }
    }
  }
}
