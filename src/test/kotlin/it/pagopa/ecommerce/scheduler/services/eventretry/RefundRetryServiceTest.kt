package it.pagopa.ecommerce.scheduler.services.eventretry

import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.mock

@ExtendWith(MockitoExtension::class)
class RefundRetryServiceTest {

  private val refundRetryQueueAsyncClient: QueueAsyncClient = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val eventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData> =
    mock()

  private val refundRetryService =
    RefundRetryService(
      refundRetryQueueAsyncClient, 10, 3, transactionsViewRepository, eventStoreRepository)

  @Test fun `Should enqueue new refund retry event for left remaining attempts`() {}
}
