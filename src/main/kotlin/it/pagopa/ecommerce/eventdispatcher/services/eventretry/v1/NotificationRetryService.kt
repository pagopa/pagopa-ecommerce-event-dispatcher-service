package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptAddRetriedEvent
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service(NotificationRetryService.QUALIFIER)
class NotificationRetryService(
  @Autowired private val notificationRetryQueueAsyncClient: QueueAsyncClient,
  @Value("\${notificationRetry.eventOffsetSeconds}") private val notificationRetryOffset: Int,
  @Value("\${notificationRetry.maxAttempts}") private val maxAttempts: Int,
  @Autowired private val viewRepository: TransactionsViewRepository,
  @Autowired
  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData>,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueuesTTLSeconds: Int
) :
  RetryEventService<TransactionUserReceiptAddRetriedEvent>(
    queueAsyncClient = notificationRetryQueueAsyncClient,
    retryOffset = notificationRetryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository,
    transientQueuesTTLSeconds = transientQueuesTTLSeconds) {

  companion object {
    const val QUALIFIER = "NotificationRetryServiceV1"
  }

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData
  ): TransactionUserReceiptAddRetriedEvent =
    TransactionUserReceiptAddRetriedEvent(transactionId.value(), transactionRetriedData)

  override fun newTransactionStatus(): TransactionStatusDto =
    TransactionStatusDto.NOTIFICATION_ERROR

  override fun validateRetryEventVisibilityTimeout(
    baseTransaction: BaseTransaction,
    visibilityTimeout: Duration
  ) = true
}
