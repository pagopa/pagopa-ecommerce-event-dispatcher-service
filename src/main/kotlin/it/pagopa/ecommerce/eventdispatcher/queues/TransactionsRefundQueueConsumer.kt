package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.serializer.json.jackson.JacksonJsonSerializer
import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.documents.v1.TransactionExpiredEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRequestedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundedData
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRefundRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

private class RefundQueueEvent(
  private val refundRequestedEvent: QueueEvent<TransactionRefundRequestedEvent>?,
  private val refundRetriedEvent: QueueEvent<TransactionRefundRetriedEvent>?,
  private val expiredEvent: QueueEvent<TransactionExpiredEvent>?
) {
  companion object {
    fun fromRefundRequested(
      refundRequestedEvent: QueueEvent<TransactionRefundRequestedEvent>
    ): RefundQueueEvent = RefundQueueEvent(refundRequestedEvent, null, null)

    fun fromRefundRetried(
      refundRetriedEvent: QueueEvent<TransactionRefundRetriedEvent>
    ): RefundQueueEvent = RefundQueueEvent(null, refundRetriedEvent, null)

    fun fromExpired(expiredEvent: QueueEvent<TransactionExpiredEvent>): RefundQueueEvent =
      RefundQueueEvent(null, null, expiredEvent)
  }

  init {
    /*
     * Require that only one variant is not null
     */
    val conditions =
      listOf(refundRequestedEvent != null, refundRetriedEvent != null, expiredEvent != null)

    require(conditions.filter { it }.size == 1) {
      "Only one variant can be non-null! Initializer: $refundRetriedEvent $refundRequestedEvent $expiredEvent"
    }
  }

  fun <T> fold(
    onRefundRequested: (QueueEvent<TransactionRefundRequestedEvent>) -> T,
    onRefundRetried: (QueueEvent<TransactionRefundRetriedEvent>) -> T,
    onExpired: (QueueEvent<TransactionExpiredEvent>) -> T
  ): T {
    return if (refundRequestedEvent != null) {
      onRefundRequested(refundRequestedEvent)
    } else if (refundRetriedEvent != null) {
      onRefundRetried(refundRetriedEvent)
    } else if (expiredEvent != null) {
      onExpired(expiredEvent)
    } else {
      throw RuntimeException("Violated invariant: one variant must be initialized")
    }
  }
}

/**
 * Event consumer for transactions to refund. These events are input in the event queue only when a
 * transaction is stuck in an REFUND_REQUESTED state **and** needs to be reverted
 */
@Service
class TransactionsRefundQueueConsumer(
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
  @Autowired private val openTelemetry: OpenTelemetry,
  @Autowired private val tracer: Tracer,
  @Autowired private val jsonSerializer: JacksonJsonSerializer
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsRefundQueueConsumer::class.java)

  private fun parseEvent(data: BinaryData): Mono<RefundQueueEvent> {
    val refundRequestedEvent =
      data.toObjectAsync(
        object : TypeReference<QueueEvent<TransactionRefundRequestedEvent>>() {}, jsonSerializer)
    val refundRetriedEvent =
      data.toObjectAsync(
        object : TypeReference<QueueEvent<TransactionRefundRetriedEvent>>() {}, jsonSerializer)
    val expiredEvent =
      data.toObjectAsync(
        object : TypeReference<QueueEvent<TransactionExpiredEvent>>() {}, jsonSerializer)

    return refundRequestedEvent
      .map(RefundQueueEvent::fromRefundRequested)
      .onErrorResume { refundRetriedEvent.map(RefundQueueEvent::fromRefundRetried) }
      .onErrorResume { expiredEvent.map(RefundQueueEvent::fromExpired) }
  }

  private fun getTransactionIdFromPayload(event: RefundQueueEvent): String {
    return event.fold(
      { it.event.transactionId }, { it.event.transactionId }, { it.event.transactionId })
  }

  @ServiceActivator(inputChannel = "transactionsrefundchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Void> {
    val binaryData = BinaryData.fromBytes(payload)
    val queueEvent = parseEvent(binaryData)
    val transactionId = queueEvent.map { getTransactionIdFromPayload(it) }

    val refundPipeline =
      transactionId
        .flatMapMany {
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(it)
        }
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
        .filter { it.status == TransactionStatusDto.REFUND_REQUESTED }
        .switchIfEmpty {
          return@switchIfEmpty transactionId
            .doOnNext {
              logger.info("Transaction $it was not previously authorized. No refund needed")
            }
            .flatMap { Mono.empty() }
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
            paymentGatewayClient,
            refundRetryService)
        }

    return queueEvent.flatMap { e ->
      val event = e.fold({ it }, { it }, { it })

      runTracedPipelineWithDeadLetterQueue(
        checkPointer,
        refundPipeline,
        event,
        deadLetterQueueAsyncClient,
        deadLetterTTLSeconds,
        openTelemetry,
        tracer,
        this::class.simpleName!!)
    }
  }
}
