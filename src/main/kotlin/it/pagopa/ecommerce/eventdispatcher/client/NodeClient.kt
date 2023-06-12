package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class NodeClient(@Autowired private val nodeApi: NodoApi) {

  companion object {
    const val CLIENT_ID_CLOSE_PAYMENT: String = "ecomm"
  }

  suspend fun closePayment(
    closePaymentRequest: ClosePaymentRequestV2Dto
  ): Mono<ClosePaymentResponseDto> {
    return mono {
      try {
        return@mono nodeApi
          .closePaymentV2(closePaymentRequest, CLIENT_ID_CLOSE_PAYMENT)
          .awaitSingle()
      } catch (exception: WebClientResponseException) {
        throw when (exception.statusCode) {
          HttpStatus.NOT_FOUND ->
            TransactionNotFound(UUID.fromString(closePaymentRequest.transactionId))
          HttpStatus.REQUEST_TIMEOUT -> GatewayTimeoutException()
          HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
          else -> exception
        }
      }
    }
  }
}
