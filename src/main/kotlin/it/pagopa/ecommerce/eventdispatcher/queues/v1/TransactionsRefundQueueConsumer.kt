package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRequestedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundedData
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRefundRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import java.util.*
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
  @Autowired private val tracingUtils: TracingUtils,
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsRefundQueueConsumer::class.java)

  private fun parseEvent(
    data: BinaryData
  ): Mono<
    Pair<Either<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>, TracingInfo?>> {
    val refundRequestedEvent =
      data
        .toObjectAsync(object : TypeReference<QueueEvent<TransactionRefundRequestedEvent>>() {})
        .map {
          Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(it.event) to
            it.tracingInfo
        }

    val refundRetriedEvent =
      data
        .toObjectAsync(object : TypeReference<QueueEvent<TransactionRefundRetriedEvent>>() {})
        .map {
          Either.left<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(it.event) to
            it.tracingInfo
        }

    val untracedRefundRequestedEvent =
      data.toObjectAsync(object : TypeReference<TransactionRefundRequestedEvent>() {}).map {
        Either.right<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(it) to null
      }

    val untracedRefundRetriedEvent =
      data.toObjectAsync(object : TypeReference<TransactionRefundRetriedEvent>() {}).map {
        Either.left<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>(it) to null
      }

    return Mono.firstWithValue(
      refundRequestedEvent,
      refundRetriedEvent,
      untracedRefundRequestedEvent,
      untracedRefundRetriedEvent)
  }

  private fun getTransactionIdFromPayload(
    event: Either<TransactionRefundRetriedEvent, TransactionRefundRequestedEvent>
  ): String {
    return event.fold({ it.transactionId }, { it.transactionId })
  }

  @ServiceActivator(inputChannel = "transactionsrefundchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Void> {
    val binaryData = BinaryData.fromBytes(payload)
    val eventData = parseEvent(binaryData)
    val transactionId = eventData.map { getTransactionIdFromPayload(it.first) }

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
        .zipWith(eventData, ::Pair)
        .flatMap { (tx, eventData) ->
          val tracingInfo = eventData.second

          refundTransaction(
            tx,
            transactionsRefundedEventStoreRepository,
            transactionsViewRepository,
            paymentGatewayClient,
            refundRetryService,
            tracingInfo)
        }

    return eventData
      .onErrorMap { InvalidEventException(payload) }
      .flatMap { (e, tracingInfo) ->
        if (tracingInfo != null) {
          val event = e.fold({ QueueEvent(it, tracingInfo) }, { QueueEvent(it, tracingInfo) })

          runTracedPipelineWithDeadLetterQueue(
            checkPointer,
            refundPipeline,
            event,
            deadLetterQueueAsyncClient,
            deadLetterTTLSeconds,
            tracingUtils,
            this::class.simpleName!!)
        } else {
          val event =
            e.fold({ BinaryData.fromObject(it).toBytes() }, { BinaryData.fromObject(it).toBytes() })

          runPipelineWithDeadLetterQueue(
            checkPointer, refundPipeline, event, deadLetterQueueAsyncClient, deadLetterTTLSeconds)
        }
      }
      .onErrorResume(InvalidEventException::class.java) {
        logger.error("Invalid input event", it)
        runPipelineWithDeadLetterQueue(
          checkPointer, refundPipeline, payload, deadLetterQueueAsyncClient, deadLetterTTLSeconds)
      }
  }
}
