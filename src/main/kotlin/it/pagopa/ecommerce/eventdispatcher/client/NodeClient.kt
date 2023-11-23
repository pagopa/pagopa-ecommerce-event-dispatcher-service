package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadClosePaymentRequest
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class NodeClient(
  @Autowired private val nodeApi: NodoApi,
  @Value("\${nodo.ecommerce.clientId}") private val ecommerceClientId: String
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  suspend fun closePayment(
    closePaymentRequest: ClosePaymentRequestV2Dto
  ): Mono<ClosePaymentResponseDto> {
    return mono {
      try {
        return@mono nodeApi
          .closePaymentV2(closePaymentRequest, ecommerceClientId)
          .doOnNext {
            logger.info(
              "Nodo close payment response for transactionId: [{}], received outcome: [{}]",
              closePaymentRequest.transactionId,
              it.outcome)
          }
          .awaitSingle()
      } catch (exception: WebClientResponseException) {
        logger.error(
          "Error performing Nodo close payment for transactionId: [{}}, received HTTP error code: [{}], response body: [{}]",
          closePaymentRequest.transactionId,
          exception.statusCode,
          exception.responseBodyAsString)
        throw when (exception.statusCode) {
          HttpStatus.NOT_FOUND ->
            TransactionNotFound(TransactionId(closePaymentRequest.transactionId).uuid)
          HttpStatus.BAD_REQUEST -> BadClosePaymentRequest("")
          HttpStatus.REQUEST_TIMEOUT -> GatewayTimeoutException()
          HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
          else -> exception
        }
      }
    }
  }
}
