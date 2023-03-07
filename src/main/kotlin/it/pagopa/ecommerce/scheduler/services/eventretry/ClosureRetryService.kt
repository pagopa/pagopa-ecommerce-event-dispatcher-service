package it.pagopa.ecommerce.scheduler.services.eventretry

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.TransactionClosureRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
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
        retryEventStoreRepository = eventStoreRepository
    ) {

    override fun buildRetryEvent(
        transactionId: TransactionId,
        transactionRetriedData: TransactionRetriedData
    ): TransactionClosureRetriedEvent =
        TransactionClosureRetriedEvent(transactionId.value.toString(), transactionRetriedData)

    override fun newTransactionStatus(): TransactionStatusDto = TransactionStatusDto.CLOSURE_ERROR
}
