package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRetriedData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service(RefundRetryService.QUALFIER)
class RefundRetryService(
  @Autowired private val refundRetryQueueAsyncClient: QueueAsyncClient,
  @Value("\${refundRetry.eventOffsetSeconds}") private val refundRetryOffset: Int,
  @Value("\${refundRetry.maxAttempts}") private val maxAttempts: Int,
  @Autowired private val viewRepository: TransactionsViewRepository,
  @Autowired
  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData>,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueuesTTLSeconds: Int,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) :
  RetryEventService<TransactionRefundRetriedEvent>(
    queueAsyncClient = refundRetryQueueAsyncClient,
    retryOffset = refundRetryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository,
    transientQueuesTTLSeconds = transientQueuesTTLSeconds,
    strictSerializerProviderV2 = strictSerializerProviderV2) {

  companion object {
    const val QUALFIER = "RefundRetryServiceV2"
  }

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData
  ): TransactionRefundRetriedEvent =
    TransactionRefundRetriedEvent(transactionId.value(), transactionRetriedData)

  override fun newTransactionStatus(): TransactionStatusDto = TransactionStatusDto.REFUND_ERROR
  override fun validateRetryEventVisibilityTimeout(
    baseTransaction: BaseTransaction,
    visibilityTimeout: Duration
  ) = true
}
