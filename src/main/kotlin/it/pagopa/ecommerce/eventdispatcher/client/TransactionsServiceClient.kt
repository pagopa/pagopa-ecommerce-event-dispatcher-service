package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.transactionauthrequests.v1.api.TransactionsApi
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class TransactionsServiceClient(
  @Autowired
  @Qualifier("transactionsServiceWebClient")
  private val transactionsServiceClient: TransactionsApi
) {

  fun patchAuthRequest(
    transactionId: UUID,
    updateAuthorizationRequestDto: UpdateAuthorizationRequestDto
  ): Mono<TransactionInfoDto> {
    return transactionsServiceClient
      .updateTransactionAuthorization(transactionId.toString(), updateAuthorizationRequestDto)
      .onErrorMap(WebClientResponseException::class.java) { exception: WebClientResponseException ->
        when (exception.statusCode) {
          HttpStatus.BAD_REQUEST -> TransactionNotFound(transactionId)
          HttpStatus.UNAUTHORIZED -> TransactionNotFound(transactionId)
          HttpStatus.NOT_FOUND -> TransactionNotFound(transactionId)
          HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
          HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
          HttpStatus.BAD_GATEWAY -> BadGatewayException("")
          else -> exception
        }
      }
  }
}
