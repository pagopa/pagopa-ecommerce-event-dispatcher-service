package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.transactionauthrequests.v1.api.TransactionsApi
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
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
  ): Mono<TransactionInfoDto> {
    return transactionsApi
      .updateTransactionAuthorization(transactionId.base64(), updateAuthorizationRequestDto)
      .onErrorMap(WebClientResponseException::class.java) { exception: WebClientResponseException ->
        when (exception.statusCode) {
          HttpStatus.BAD_REQUEST ->
            PatchAuthRequestErrorResponseException(
              transactionId.uuid, exception.statusCode, exception.responseBodyAsString)
          HttpStatus.UNAUTHORIZED ->
            UnauthorizedPatchAuthorizationRequestException(transactionId.uuid, exception.statusCode)
          HttpStatus.NOT_FOUND -> TransactionNotFound(transactionId.uuid)
          HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
          HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
          HttpStatus.BAD_GATEWAY -> BadGatewayException("")
          else -> exception
        }
      }
  }
}
