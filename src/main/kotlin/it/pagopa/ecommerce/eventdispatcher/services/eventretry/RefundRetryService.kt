package it.pagopa.ecommerce.eventdispatcher.services.eventretry

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class RefundRetryService(
  @Autowired private val refundRetryQueueAsyncClient: QueueAsyncClient,
  @Value("\${refundRetry.eventOffsetSeconds}") private val refundRetryOffset: Int,
  @Value("\${refundRetry.maxAttempts}") private val maxAttempts: Int,
  @Autowired private val viewRepository: TransactionsViewRepository,
  @Autowired
  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData>
) :
  RetryEventService<TransactionRefundRetriedEvent>(
    queueAsyncClient = refundRetryQueueAsyncClient,
    retryOffset = refundRetryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository) {

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData
  ): TransactionRefundRetriedEvent =
    TransactionRefundRetriedEvent(transactionId.value(), transactionRetriedData)

  override fun newTransactionStatus(): TransactionStatusDto = TransactionStatusDto.REFUND_ERROR
}
