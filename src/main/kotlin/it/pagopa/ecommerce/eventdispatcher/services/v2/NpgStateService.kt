package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithPaymentToken
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GetStateException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.QueueCommonsLogger.logger
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

@Component
class NpgStateService(
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgCardsPspApiKey: NpgPspApiKeysConfig,
  @Value("\${transactionsAuthRequestedRetry.paymentTokenValidityTimeOffset}") private val paymentTokenValidityTimeOffset: Int,
) {

  fun getStateNpg(transactionId: UUID, sessionId: String, pspId: String): Mono<StateResponseDto> {
    return npgCardsPspApiKey[pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        npgClient.getState(UUID.randomUUID(), sessionId, apiKey).onErrorMap(
          NpgResponseException::class.java) { exception: NpgResponseException ->
          val responseStatusCode = exception.statusCode
          responseStatusCode
            .map {
              val errorCodeReason = "Received HTTP error code from NPG: $it"
              if (it.is5xxServerError) {
                BadGatewayException(errorCodeReason)
              } else {
                GetStateException(transactionId, errorCodeReason)
              }
            }
            .orElse(GetStateException(transactionId, "Unknown NPG HTTP response code"))
        }
      })
  }


  fun validateRetryEventVisibilityTimeout(
    baseTransaction: BaseTransaction,
    visibilityTimeout: Duration
  ): Boolean {
    val paymentTokenValidityOffset = Duration.ofSeconds(paymentTokenValidityTimeOffset.toLong())
    val paymentTokenDuration = getPaymentTokenDuration(baseTransaction)
    val paymentTokenValidityEnd =
      baseTransaction.creationDate.plus(paymentTokenDuration).toInstant()
    val retryEventVisibilityInstant = Instant.now().plus(visibilityTimeout)
    // Performing check against payment token validity end and retry event visibility timeout
    // A configurable paymentTokenValidityOffset is added to retry event in order to avoid sending
    // retry event too
    // closer to the payment token end
    val paymentTokenStillValidAtRetry =
      paymentTokenValidityEnd.minus(paymentTokenValidityOffset).isAfter(retryEventVisibilityInstant)
    if (!paymentTokenStillValidAtRetry) {
      logger.warn(
        "No closure retry event send for transaction with id: [${baseTransaction.transactionId.value()}]. Retry event visibility timeout: [$retryEventVisibilityInstant], will be after payment token validity end: [$paymentTokenValidityEnd] with offset: [$paymentTokenValidityOffset]")
    }
    return paymentTokenStillValidAtRetry
  }

  private fun getPaymentTokenDuration(baseTransaction: BaseTransaction): Duration =
    Duration.ofSeconds(
      (baseTransaction as BaseTransactionWithPaymentToken)
        .transactionActivatedData
        .paymentTokenValiditySeconds
        .toLong())

}
