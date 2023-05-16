package it.pagopa.ecommerce.eventdispatcher.services.eventretry

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.TransactionClosureRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import java.time.Instant
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class ClosureRetryService(
  @Autowired private val closureRetryQueueAsyncClient: QueueAsyncClient,
  @Value("\${closePaymentRetry.eventOffsetSeconds}") private val closePaymentRetryOffset: Int,
  @Value("\${closePaymentRetry.maxAttempts}") private val maxAttempts: Int,
  @Autowired private val viewRepository: TransactionsViewRepository,
  @Autowired
  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData>
) :
  RetryEventService<TransactionClosureRetriedEvent>(
    queueAsyncClient = closureRetryQueueAsyncClient,
    retryOffset = closePaymentRetryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository) {

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData
  ): TransactionClosureRetriedEvent =
    TransactionClosureRetriedEvent(transactionId.value(), transactionRetriedData)

  override fun newTransactionStatus(): TransactionStatusDto = TransactionStatusDto.CLOSURE_ERROR
  override fun validateRetryEventVisibilityTimeout(
    baseTransaction: BaseTransaction,
    visibilityTimeout: Duration
  ): Boolean {
    val offset = Duration.ofSeconds(10)
    val paymentTokenDuration = Duration.ofMinutes(15)
    val paymentTokenValidityEnd =
      baseTransaction.creationDate.plus(paymentTokenDuration).toInstant()
    val retryEventVisibilityInstant = Instant.now().plus(visibilityTimeout)
    val paymentTokenStillValidAtRetry =
      paymentTokenValidityEnd.isAfter(retryEventVisibilityInstant.plus(offset))
    if (!paymentTokenStillValidAtRetry) {
      logger.info(
        "No closure retry event send for transaction with id: [${baseTransaction.transactionId}] because retry event visibility timeout: [$retryEventVisibilityInstant], will be after payment token validity end: [$paymentTokenValidityEnd].")
    }
    return paymentTokenStillValidAtRetry
  }
}
