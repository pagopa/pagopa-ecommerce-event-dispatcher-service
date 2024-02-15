package it.pagopa.ecommerce.eventdispatcher.services.v2

import com.azure.core.util.BinaryData
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithPaymentToken
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GetStateException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TooLateRetryAttemptException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.QueueCommonsLogger.logger
import java.time.Duration
import java.time.Instant
import java.util.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

@Component
class NpgStateService(
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgCardsPspApiKey: NpgPspApiKeysConfig,
  @Value("\${transactionAuthorizationRequested.paymentTokenValidityTimeOffset}")
  private val paymentTokenValidityTimeOffset: Int,
  @Autowired private val authRequestedQueueAsyncClient: QueueAsyncClient,
  @Value("\${transactionAuthorizationRequested.eventOffsetSeconds}") private val retryOffset: Int,
  @Value("\${transactionAuthorizationRequested.maxAttempts}") private val maxAttempts: Int,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueuesTTLSeconds: Int,
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

  fun enqueueRetryEvent(
    baseTransaction: BaseTransaction,
    retryCount: Int,
    retryEvent: TransactionAuthorizationRequestedEvent,
    tracingInfo: TracingInfo?
  ): Mono<BaseTransaction> {
    val visibilityTimeout = Duration.ofSeconds((retryOffset * retryCount).toLong())
    return Mono.just(baseTransaction)
      .filter { retryCount <= maxAttempts }
      .switchIfEmpty(
        Mono.error(
          NoRetryAttemptsLeftException(
            eventCode = retryEvent.eventCode, transactionId = baseTransaction.transactionId)))
      .filter { validateRetryEventVisibilityTimeout(baseTransaction, visibilityTimeout) }
      .switchIfEmpty {
        Mono.error(
          TooLateRetryAttemptException(
            eventCode = retryEvent.eventCode,
            transactionId = baseTransaction.transactionId,
            visibilityTimeout = Instant.now().plus(visibilityTimeout)))
      }
      .flatMap {
        authRequestedQueueAsyncClient.sendMessageWithResponse(
          BinaryData.fromObject(QueueEvent(retryEvent, tracingInfo)),
          visibilityTimeout,
          Duration.ofSeconds(transientQueuesTTLSeconds.toLong()))
      }
      .doOnError {
        logger.error(
          "Error processing retry event for authorization requested transaction with id: [${retryEvent.transactionId}]",
          it)
      }
      .flatMap { Mono.just(baseTransaction) }
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
        "No get state retry event send for transaction with id: [${baseTransaction.transactionId.value()}]. Retry event visibility timeout: [$retryEventVisibilityInstant], will be after payment token validity end: [$paymentTokenValidityEnd] with offset: [$paymentTokenValidityOffset]")
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
