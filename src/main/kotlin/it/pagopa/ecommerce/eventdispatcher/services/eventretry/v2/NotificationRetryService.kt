package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptAddRetriedEvent
import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
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
  private val eventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRetriedData>,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueuesTTLSeconds: Int,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) :
  RetryEventService<TransactionEvent<BaseTransactionRetriedData>>(
    queueAsyncClient = notificationRetryQueueAsyncClient,
    retryOffset = notificationRetryOffset,
    maxAttempts = maxAttempts,
    viewRepository = viewRepository,
    retryEventStoreRepository = eventStoreRepository,
    transientQueuesTTLSeconds = transientQueuesTTLSeconds,
    strictSerializerProviderV2 = strictSerializerProviderV2) {

  companion object {
    const val QUALIFIER = "NotificationRetryServiceV2"
  }

  override fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData,
    transactionGatewayAuthorizationData: TransactionGatewayAuthorizationData?,
    throwable: Throwable?
  ): TransactionEvent<BaseTransactionRetriedData> =
    TransactionUserReceiptAddRetriedEvent(transactionId.value(), transactionRetriedData)
      as TransactionEvent<BaseTransactionRetriedData>

  override fun newTransactionStatus(): TransactionStatusDto =
    TransactionStatusDto.NOTIFICATION_ERROR

  override fun validateRetryEventVisibilityTimeout(
    baseTransaction: BaseTransaction,
    visibilityTimeout: Duration
  ) = true
}
