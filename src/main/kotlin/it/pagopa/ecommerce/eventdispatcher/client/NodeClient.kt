package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.server.ResponseStatusException
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
    logger.info(
      "Requested closePaymentV2 for transactionId [{}]: paymentTokens {} - outcome: {}",
      closePaymentRequest.transactionId,
      closePaymentRequest.paymentTokens,
      closePaymentRequest.outcome.value)
    return nodeApi.apiClient.webClient
      .post()
      .uri { it.path("/closepayment").queryParam("clientId", ecommerceClientId).build() }
      .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
      .body(mono { closePaymentRequest }, ClosePaymentRequestV2Dto::class.java)
      .retrieve()
      .onStatus(
        { obj: HttpStatus -> obj.isError },
        { clientResponse: ClientResponse ->
          clientResponse.bodyToMono(String::class.java).switchIfEmpty(Mono.just("N/A")).flatMap {
            errorResponseBodyAsString: String ->
            Mono.error(
              ResponseStatusException(clientResponse.statusCode(), errorResponseBodyAsString))
          }
        })
      .bodyToMono(ClosePaymentResponseDto::class.java)
      .doOnSuccess { closePaymentResponse: ClosePaymentResponseDto ->
        logger.info(
          "Received closePaymentV2 response for transactionId [{}]: paymentTokens {} - outcome: {}",
          closePaymentRequest.transactionId,
          closePaymentRequest.paymentTokens,
          closePaymentResponse.outcome)
      }
      .onErrorMap { exception ->
        logger.error(
          "Received closePaymentV2 Response Status Error for transactionId [${closePaymentRequest.transactionId}]",
          exception)
        if (exception is ResponseStatusException) {
          throw ClosePaymentErrorResponseException(
            exception.status,
            runCatching {
                nodeApi.apiClient.objectMapper.readValue(exception.reason, ErrorDto::class.java)
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
