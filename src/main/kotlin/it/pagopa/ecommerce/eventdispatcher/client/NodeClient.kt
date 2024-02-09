package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class NodeClient(
  @Autowired private val nodeApi: NodoApi,
  @Value("\${nodo.ecommerce.clientId}") private val ecommerceClientId: String
) {

  companion object {
    const val NODE_DID_NOT_RECEIVE_RPT_YET_ERROR = "Node did not receive RPT yet"
  }

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
      } catch (exception: Exception) {
        logger.error(
          "Error performing Nodo close payment for transactionId: [${closePaymentRequest.transactionId}]",
          exception)
        if (exception is WebClientResponseException) {
          throw ClosePaymentErrorResponseException(
            exception.statusCode,
            runCatching {
                nodeApi.apiClient.objectMapper.readValue(
                  exception.responseBodyAsString, ErrorDto::class.java)
              }
              .onFailure {
                logger.error("Error parsing Nodo close payment error response body", it)
              }
              .getOrNull())
        } else {
          throw ClosePaymentErrorResponseException(null, null)
        }
      }
    }
  }
}
