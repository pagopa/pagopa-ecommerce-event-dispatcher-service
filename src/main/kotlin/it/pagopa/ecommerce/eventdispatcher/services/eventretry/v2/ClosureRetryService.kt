package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithPaymentToken
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentEvent
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import java.time.Instant
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service(ClosureRetryService.QUALIFIER)
class ClosureRetryService(
  @Autowired private val closureRetryQueueAsyncClient: QueueAsyncClient,
  @Value("\${closePaymentRetry.eventOffsetSeconds}") private val closePaymentRetryOffset: Int,
  @Value("\${closePaymentRetry.maxAttempts}") private val maxAttempts: Int,
  @Autowired private val viewRepository: TransactionsViewRepository,
  @Autowired
  private val eventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRetriedData>,
  @Value("\${closePaymentRetry.paymentTokenValidityTimeOffset}")
  private val paymentTokenValidityTimeOffset: Int,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueuesTTLSeconds: Int,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) :
  RetryEventService<TransactionEvent<BaseTransactionRetriedData>>(
    queueAsyncClient = closureRetryQueueAsyncClient,
    retryOffset = closePaymentRetryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository,
    transientQueuesTTLSeconds = transientQueuesTTLSeconds,
    strictSerializerProviderV2 = strictSerializerProviderV2) {

  companion object {
    const val QUALIFIER = "ClosureRetryServiceV2"
  }

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData,
    transactionGatewayAuthorizationData: TransactionGatewayAuthorizationData?,
    throwable: Throwable?
  ): TransactionEvent<BaseTransactionRetriedData> =
    TransactionClosureRetriedEvent(
      transactionId.value(),
      TransactionClosureRetriedData(
        throwable?.let { ClosePaymentEvent.exceptionToClosureErrorData(it) },
        transactionRetriedData.retryCount))
      as TransactionEvent<BaseTransactionRetriedData>

  override fun newTransactionStatus(): TransactionStatusDto = TransactionStatusDto.CLOSURE_ERROR

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
