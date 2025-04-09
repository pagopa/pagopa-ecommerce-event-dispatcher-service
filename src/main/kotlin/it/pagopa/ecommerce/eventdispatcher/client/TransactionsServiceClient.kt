package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.generated.transactionauthrequests.v2.api.TransactionsApi
import it.pagopa.generated.transactionauthrequests.v2.dto.UpdateAuthorizationRequestDto
import it.pagopa.generated.transactionauthrequests.v2.dto.UpdateAuthorizationResponseDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class TransactionsServiceClient(
  @Autowired @Qualifier("transactionsServiceWebClient") private val transactionsApi: TransactionsApi
) {

  fun patchAuthRequest(
    transactionId: TransactionId,
    updateAuthorizationRequestDto: UpdateAuthorizationRequestDto
  ): Mono<UpdateAuthorizationResponseDto> {
    return transactionsApi
      .updateTransactionAuthorization(transactionId.value(), updateAuthorizationRequestDto)
      .onErrorMap(WebClientResponseException::class.java) { exception: WebClientResponseException ->
        when (exception.statusCode) {
          HttpStatus.BAD_REQUEST ->
            PatchAuthRequestErrorResponseException(
              transactionId, exception.statusCode, exception.responseBodyAsString)
          HttpStatus.UNAUTHORIZED ->
            UnauthorizedPatchAuthorizationRequestException(transactionId, exception.statusCode)
          HttpStatus.NOT_FOUND -> TransactionNotFound(transactionId.uuid)
          HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
          HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
          HttpStatus.BAD_GATEWAY -> BadGatewayException("")
          else -> exception
        }
      }
  }
}
