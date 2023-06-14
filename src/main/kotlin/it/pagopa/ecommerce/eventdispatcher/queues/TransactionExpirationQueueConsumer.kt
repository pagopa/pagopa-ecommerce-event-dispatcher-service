package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Headers
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to transaction activation. This consumer's responsibilities are
 * to handle expiration of transactions and subsequent refund for transaction stuck in a
 * pending/transient state.
 */
@Service
class TransactionExpirationQueueConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsExpiredEventStoreRepository:
    TransactionsEventStoreRepository<TransactionExpiredData>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val transactionUtils: TransactionUtils,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Autowired private val expirationQueueAsyncClient: QueueAsyncClient,
  @Value("\${sendPaymentResult.timeoutSeconds}") private val sendPaymentResultTimeoutSeconds: Int,
  @Value("\${sendPaymentResult.expirationOffset}")
  private val sendPaymentResultTimeoutOffsetSeconds: Int,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueueTTLSeconds: Int,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val openTelemetry: OpenTelemetry,
  @Autowired private val tracer: Tracer
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionExpirationQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData): String {
    return data.toObject(TransactionActivatedEvent::class.java).transactionId
  }

  @ServiceActivator(inputChannel = "transactionexpiredchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer,
    @Headers headers: MessageHeaders
  ): Mono<Void> {
    val binaryData = BinaryData.fromBytes(payload)
    val transactionId = getTransactionIdFromPayload(binaryData)
    val events =
      transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(transactionId)
    val baseTransaction = reduceEvents(events)
    val refundPipeline =
      baseTransaction
        .filter {
          val isTransient = transactionUtils.isTransientStatus(it.status)
          logger.info(
            "Transaction ${it.transactionId.value()} in status ${it.status}, is transient: $isTransient")
          isTransient
        }
        .filterWhen {
          val sendPaymentResultTimeLeft =
            timeLeftForSendPaymentResult(it, sendPaymentResultTimeoutSeconds, events)
          sendPaymentResultTimeLeft
            .flatMap { timeLeft ->
              val sendPaymentResultOffset =
                Duration.ofSeconds(sendPaymentResultTimeoutOffsetSeconds.toLong())
              val expired = timeLeft < sendPaymentResultOffset
              logger.info(
                "Transaction ${it.transactionId.value()} - Time left for send payment result: $timeLeft, timeout offset: $sendPaymentResultOffset  --> expired: $expired")
              if (expired) {
                logger.error(
                  "Transaction ${it.transactionId.value()} - No send payment result received on time! Transaction will be expired.")
                deadLetterQueueAsyncClient
                  .sendMessageWithResponse(
                    binaryData,
                    Duration.ZERO,
                    Duration.ofSeconds(deadLetterTTLSeconds.toLong()),
                  )
                  .thenReturn(true)
              } else {
                logger.info(
                  "Transaction ${it.transactionId.value()} still waiting for sendPaymentResult outcome, expiration event sent with visibility timeout: $timeLeft")
                expirationQueueAsyncClient
                  .sendMessageWithResponse(
                    binaryData,
                    timeLeft,
                    Duration.ofSeconds(transientQueueTTLSeconds.toLong()),
                  )
                  .thenReturn(false)
              }
            }
            .switchIfEmpty(Mono.just(true))
        }
        .flatMap { tx ->
          val isTransactionExpired = isTransactionExpired(tx)
          logger.info("Transaction ${tx.transactionId.value()} is expired: $isTransactionExpired")
          if (!isTransactionExpired) {
            updateTransactionToExpired(
              tx, transactionsExpiredEventStoreRepository, transactionsViewRepository)
          } else {
            Mono.just(tx)
          }
        }
        .filter {
          val refundable = isTransactionRefundable(it)
          logger.info(
            "Transaction ${it.transactionId.value()} in status ${it.status}, refundable: $refundable")
          refundable
        }
        .flatMap {
          updateTransactionToRefundRequested(
            it, transactionsRefundedEventStoreRepository, transactionsViewRepository)
        }
        .flatMap { tx ->
          val transaction =
            if (tx is TransactionWithClosureError) {
              tx.transactionAtPreviousState
            } else {
              tx
            }
          refundTransaction(
            transaction,
            transactionsRefundedEventStoreRepository,
            transactionsViewRepository,
            paymentGatewayClient,
            refundRetryService)
        }

    return runPipelineWithDeadLetterQueue(
      checkPointer,
      refundPipeline,
      payload,
      deadLetterQueueAsyncClient,
      deadLetterTTLSeconds,
      openTelemetry,
      headers,
      tracer,
      this::class.simpleName!!)
  }
}
