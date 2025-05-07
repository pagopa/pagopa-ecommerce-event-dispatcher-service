package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationOutcomeWaitingEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithPaymentToken
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import java.time.Instant
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class AuthorizationStateRetrieverRetryService(
  @Value("\${transactionAuthorizationOutcomeWaiting.paymentTokenValidityTimeOffsetSeconds}")
  private val paymentTokenValidityTimeOffset: Int,
  @Autowired private val authRequestedOutcomeWaitingQueueAsyncClient: QueueAsyncClient,
  @Autowired private val viewRepository: TransactionsViewRepository,
  @Autowired
  private val eventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRetriedData>,
  @Value("\${transactionAuthorizationOutcomeWaiting.eventOffsetSeconds}")
  private val retryOffset: Int,
  @Value("\${transactionAuthorizationOutcomeWaiting.maxAttempts}") private val maxAttempts: Int,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueuesTTLSeconds: Int,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) :
  RetryEventService<TransactionEvent<BaseTransactionRetriedData>>(
    queueAsyncClient = authRequestedOutcomeWaitingQueueAsyncClient,
    retryOffset = retryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository,
    transientQueuesTTLSeconds = transientQueuesTTLSeconds,
    strictSerializerProviderV2 = strictSerializerProviderV2) {

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData,
    transactionGatewayAuthorizationData: TransactionGatewayAuthorizationData?,
    throwable: Throwable?
  ): TransactionEvent<BaseTransactionRetriedData> =
    TransactionAuthorizationOutcomeWaitingEvent(transactionId.value(), transactionRetriedData)
      as TransactionEvent<BaseTransactionRetriedData>

  override fun newTransactionStatus(): TransactionStatusDto =
    TransactionStatusDto.AUTHORIZATION_REQUESTED

  override fun validateRetryEventVisibilityTimeout(
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
