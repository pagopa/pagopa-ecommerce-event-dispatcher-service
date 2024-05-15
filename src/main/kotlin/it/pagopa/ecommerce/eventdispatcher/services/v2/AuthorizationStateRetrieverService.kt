package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.client.NpgClient.PaymentMethod
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OrderResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.utils.NpgApiKeyConfiguration
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidNPGPaymentGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgBadRequestException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgServerErrorException
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class AuthorizationStateRetrieverService(
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgApiKeyConfiguration: NpgApiKeyConfiguration,
) {

  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  fun getStateNpg(
    transactionId: TransactionId,
    sessionId: String,
    pspId: String,
    correlationId: String,
    paymentMethod: PaymentMethod
  ): Mono<StateResponseDto> {
    return npgApiKeyConfiguration[paymentMethod, pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        npgClient.getState(UUID.fromString(correlationId), sessionId, apiKey).onErrorMap(
          NpgResponseException::class.java) { exception: NpgResponseException ->
          val responseStatusCode = exception.statusCode
          responseStatusCode
            .map {
              val errorCodeReason = "Received HTTP error code from NPG: $it"
              if (it.is5xxServerError) {
                NpgServerErrorException(errorCodeReason)
              } else {
                NpgBadRequestException(transactionId, errorCodeReason)
              }
            }
            .orElse(exception)
        }
      })
  }

  fun getOrder(
    trx: BaseTransaction,
  ): Mono<OrderResponseDto> {
    return Mono.just(trx)
      .cast(BaseTransactionWithRequestedAuthorization::class.java)
      .filter { transaction ->
        transaction.transactionAuthorizationRequestData.paymentGateway ==
          TransactionAuthorizationRequestData.PaymentGateway.NPG
      }
      .switchIfEmpty(Mono.error(InvalidNPGPaymentGatewayException(trx.transactionId)))
      .flatMap { transaction ->
        val activationData =
          (transaction.transactionActivatedData.transactionGatewayActivationData
            as NpgTransactionGatewayActivationData)
        getOrder(
          transactionId = trx.transactionId,
          pspId = transaction.transactionAuthorizationRequestData.pspId,
          paymentMethod =
            NpgClient.PaymentMethod.valueOf(
              transaction.transactionAuthorizationRequestData.paymentMethodName),
          orderId = activationData.orderId,
          correlationId = activationData.correlationId)
      }
  }

  private fun getOrder(
    transactionId: TransactionId,
    orderId: String,
    pspId: String,
    correlationId: String,
    paymentMethod: PaymentMethod
  ): Mono<OrderResponseDto> {
    return npgApiKeyConfiguration[paymentMethod, pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        npgClient.getOrder(UUID.fromString(correlationId), apiKey, orderId).onErrorMap(
          NpgResponseException::class.java) { exception: NpgResponseException ->
          val responseStatusCode = exception.statusCode
          responseStatusCode
            .map {
              val errorCodeReason = "Received HTTP error code from NPG: $it"
              if (it.is5xxServerError) {
                NpgServerErrorException(errorCodeReason)
              } else {
                NpgBadRequestException(transactionId, errorCodeReason)
              }
            }
            .orElse(exception)
        }
      })
  }
}
