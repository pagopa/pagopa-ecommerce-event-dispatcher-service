package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRefundedData
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.Transaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRefundRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionTracing
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

/**
 * Event consumer for transactions to refund. These events are input in the event queue only when a
 * transaction is stuck in an REFUND_REQUESTED state **and** needs to be reverted
 */
@Service("TransactionsRefundQueueConsumerV2")
class TransactionsRefundQueueConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val refundService: RefundService,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val npgService: NpgService,
  @Autowired private val transactionTracing: TransactionTracing
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsRefundQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(
    event: Either<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>
  ): String {
    return event.fold({ it.transactionId }, { it.transactionId })
  }

  fun messageReceiver(
    parsedEvent:
      Either<
        QueueEvent<TransactionRefundRetriedEvent>, QueueEvent<TransactionRefundRequestedEvent>>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val event = parsedEvent.bimap({ it.event }, { it.event })
    val tracingInfo = parsedEvent.fold({ it.tracingInfo }, { it.tracingInfo })
    val transactionId = getTransactionIdFromPayload(event)

    val events =
      transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId)

    val refundPipeline =
      events
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
        .filter { it.status == TransactionStatusDto.REFUND_REQUESTED }
        .switchIfEmpty {
          logger.info("Transaction $transactionId was not previously authorized. No refund needed")
          Mono.empty()
        }
        .doOnNext {
          logger.info("Handling refund request for transaction with id ${it.transactionId.value()}")
        }
        .cast(BaseTransactionWithRefundRequested::class.java)
        .flatMap { tx ->
          refundTransaction(
            tx,
            transactionsRefundedEventStoreRepository,
            transactionsViewRepository,
            refundService,
            refundRetryService,
            npgService,
            tracingInfo,
          )
        }
        .doOnSuccess {
          if (it != null) {
            transactionTracing.addSpanAttributesRefundedFlowFromTransaction(it, events)
          }
        }
    val e = event.fold({ QueueEvent(it, tracingInfo) }, { QueueEvent(it, tracingInfo) })
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      refundPipeline,
      e,
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }
}
