package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionRefundRetryQueueConsumer
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to refund retry. This consumer's responsibilities are to handle
 * refund process retry for a given transaction
 */
@Service("TransactionRefundRetryQueueConsumerV1")
@Deprecated("Mark for deprecation in favor of V2 version")
class TransactionRefundRetryQueueConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val tracingUtils: TracingUtils
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionRefundRetryQueueConsumer::class.java)

  fun messageReceiver(
    parsedEvent: Pair<TransactionRefundRetriedEvent, TracingInfo?>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val (event, tracingInfo) = parsedEvent
    val baseTransaction =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(event.transactionId)
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
    val refundPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.REFUND_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = listOf(TransactionStatusDto.REFUND_ERROR),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .flatMap { tx ->
          refundTransaction(
            tx,
            transactionsRefundedEventStoreRepository,
            transactionsViewRepository,
            paymentGatewayClient,
            refundRetryService,
            tracingInfo,
            event.data.retryCount)
        }
    return if (tracingInfo != null) {
      runTracedPipelineWithDeadLetterQueue(
        checkPointer,
        refundPipeline,
        QueueEvent(event, tracingInfo),
        deadLetterQueueAsyncClient,
        deadLetterTTLSeconds,
        tracingUtils,
        this::class.simpleName!!)
    } else {
      runPipelineWithDeadLetterQueue(
        checkPointer,
        refundPipeline,
        BinaryData.fromObject(event).toBytes(),
        deadLetterQueueAsyncClient,
        deadLetterTTLSeconds,
      )
    }
  }
}
