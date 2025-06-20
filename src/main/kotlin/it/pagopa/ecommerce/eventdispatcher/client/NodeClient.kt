package it.pagopa.ecommerce.eventdispatcher.client

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.generated.ecommerce.nodo.v2.dto.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatusCode
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.server.ResponseStatusException
import reactor.core.publisher.Mono

@Component
class NodeClient(
  @Qualifier("nodoApiClient") private val nodoApiClient: WebClient,
  @Value("\${nodo.ecommerce.clientId}") private val ecommerceClientId: String,
  @Autowired private val objectMapper: ObjectMapper
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  fun closePayment(closePaymentRequest: ClosePaymentRequestV2Dto): Mono<ClosePaymentResponseDto> {
    val transactionId =
      when (closePaymentRequest) {
        is CardClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        is RedirectClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        is BancomatPayClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        is MyBankClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        is PayPalClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        is SatispayClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        is ApplePayClosePaymentRequestV2Dto -> closePaymentRequest.transactionId
        else ->
          throw IllegalArgumentException(
            "Unhandled `ClosePaymentRequestV2Dto` implementation: ${closePaymentRequest.javaClass}")
      }

    val paymentTokens =
      when (closePaymentRequest) {
        is CardClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        is RedirectClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        is BancomatPayClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        is MyBankClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        is PayPalClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        is SatispayClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        is ApplePayClosePaymentRequestV2Dto -> closePaymentRequest.paymentTokens
        else ->
          throw IllegalArgumentException(
            "Unhandled `ClosePaymentRequestV2Dto` implementation: ${closePaymentRequest.javaClass}")
      }

    return nodoApiClient
      .post()
      .uri { builder ->
        builder.pathSegment("closepayment").queryParam("clientId", ecommerceClientId).build()
      }
      .body(Mono.just(closePaymentRequest), ClosePaymentRequestV2Dto::class.java)
      .retrieve()
      .onStatus(
        { obj: HttpStatusCode -> obj.isError },
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
          transactionId,
          paymentTokens,
          closePaymentResponse.outcome)
      }
      .onErrorMap { exception ->
        logger.error(
          "Received closePaymentV2 Response Status Error for transactionId [$transactionId]",
          exception)
        if (exception is ResponseStatusException) {
          ClosePaymentErrorResponseException(
            exception.statusCode,
            runCatching { objectMapper.readValue(exception.reason, ErrorDto::class.java) }
              .onFailure {
                logger.error("Error parsing Nodo close payment error response body", it)
              }
              .getOrNull())
        } else {
          ClosePaymentErrorResponseException(null, null)
        }
      }
  }
}
