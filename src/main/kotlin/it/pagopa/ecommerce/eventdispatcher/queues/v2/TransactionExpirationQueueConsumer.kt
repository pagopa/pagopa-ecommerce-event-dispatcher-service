package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.utils.v2.TransactionUtils
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionTracing
import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.messaging.MessageHeaders
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to transaction activation. This consumer's responsibilities are
 * to handle expiration of transactions and subsequent refund for transaction stuck in a
 * pending/transient state.
 */
@Service("TransactionExpirationQueueConsumerV2")
class TransactionExpirationQueueConsumer(
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsExpiredEventStoreRepository:
    TransactionsEventStoreRepository<TransactionExpiredData>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val transactionUtils: TransactionUtils,
  @Autowired
  private val refundRequestedAsyncClient: it.pagopa.ecommerce.commons.client.QueueAsyncClient,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val expirationQueueAsyncClient: QueueAsyncClient,
  @Value("\${sendPaymentResult.timeoutSeconds}") private val sendPaymentResultTimeoutSeconds: Int,
  @Value("\${sendPaymentResult.expirationOffset}")
  private val sendPaymentResultTimeoutOffsetSeconds: Int,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueueTTLSeconds: Int,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val npgService: NpgService,
  @Autowired private val transactionTracing: TransactionTracing
) {

  val logger: Logger = LoggerFactory.getLogger(TransactionExpirationQueueConsumer::class.java)

  fun messageReceiver(
    queueEvent: Either<QueueEvent<TransactionActivatedEvent>, QueueEvent<TransactionExpiredEvent>>,
    checkPointer: Checkpointer,
    headers: MessageHeaders
  ): Mono<Unit> {
    val event = queueEvent.fold({ it }, { it })
    val transactionId = queueEvent.fold({ it.event.transactionId }, { it.event.transactionId })
    val events =
      Flux.defer {
        transactionsEventStoreRepository
          .findByTransactionIdOrderByCreationDateAsc(transactionId)
          .map { it as TransactionEvent<Any> }
      }
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
              val binaryData =
                BinaryData.fromObject(event, strictSerializerProviderV2.createInstance())
              val sendPaymentResultOffset =
                Duration.ofSeconds(sendPaymentResultTimeoutOffsetSeconds.toLong())
              val expired = timeLeft < sendPaymentResultOffset
              logger.info(
                "Transaction ${it.transactionId.value()} - Time left for send payment result: $timeLeft, timeout offset: $sendPaymentResultOffset  --> expired: $expired")
              if (expired) {
                logger.error(
                  "Transaction ${it.transactionId.value()} - No send payment result received on time! Transaction will be expired.")
                deadLetterTracedQueueAsyncClient
                  .sendAndTraceDeadLetterQueueEvent(
                    binaryData,
                    DeadLetterTracedQueueAsyncClient.ErrorContext(
                      transactionId = TransactionId(transactionId),
                      transactionEventCode = event.event.eventCode,
                      errorCategory =
                        DeadLetterTracedQueueAsyncClient.ErrorCategory
                          .SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT),
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
              .doOnSuccess { it ->
                transactionTracing.addSpanAttributesExpiredFlowFromTransaction(it, events)
              }
          } else {
            Mono.just(tx)
          }
        }
        .filterWhen {
          val refundableCheckRequired = isRefundableCheckRequired(it)
          val refundable = isTransactionRefundable(it)
          val refundableWithoutCheck = refundable && !refundableCheckRequired
          logger.info(
            "Transaction ${it.transactionId.value()} in status ${it.status}, refundable : $refundable, without check : $refundableWithoutCheck")
          if (refundable && refundableCheckRequired) {
            val binaryData =
              BinaryData.fromObject(event, strictSerializerProviderV2.createInstance())
            deadLetterTracedQueueAsyncClient
              .sendAndTraceDeadLetterQueueEvent(
                binaryData,
                DeadLetterTracedQueueAsyncClient.ErrorContext(
                  transactionId = TransactionId(event.event.transactionId),
                  transactionEventCode = event.event.eventCode,
                  errorCategory =
                    DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED),
              )
              .map { false }
          } else {
            Mono.just(refundableWithoutCheck)
          }
        }
        .flatMap { tx ->
          val tracingInfo = queueEvent.fold({ it.tracingInfo }, { it.tracingInfo })
          val delayForRefund =
            computeRefundProcessingRequestDelay(
              tx = tx,
              events = events,
              timeToWaitFromAuthRequestMinutes = npgService.refundDelayFromAuthRequestMinutes)
          delayForRefund.flatMap { timeout ->
            val binaryData =
              BinaryData.fromObject(event, strictSerializerProviderV2.createInstance())
            if (timeout.isZero) {
              requestRefundTransaction(
                tx, // transaction
                transactionsRefundedEventStoreRepository,
                transactionsViewRepository,
                npgService,
                tracingInfo,
                refundRequestedAsyncClient,
                Duration.ofSeconds(transientQueueTTLSeconds.toLong()))
            } else {
              logger.info(
                "Transaction ${tx.transactionId.value()} not received authorization outcome yet, postpone refund processing for: $timeout")
              expirationQueueAsyncClient
                .sendMessageWithResponse(
                  binaryData,
                  timeout + Duration.ofSeconds(npgService.eventProcessingDelaySeconds),
                  Duration.ofSeconds(transientQueueTTLSeconds.toLong()),
                )
                .thenReturn(false)
            }
          }
        }

    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      refundPipeline,
      QueueEvent(event.event, event.tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }
}
