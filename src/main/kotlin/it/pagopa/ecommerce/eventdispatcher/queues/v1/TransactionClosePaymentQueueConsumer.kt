package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.redis.templatewrappers.PaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v1.NodeService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionClosePaymentQueueConsumerV1")
@Deprecated("Mark for deprecation in favor of V2 version")
class TransactionClosePaymentQueueConsumer(
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionClosureSentEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData>,
  @Autowired
  private val transactionClosureErrorEventStoreRepository: TransactionsEventStoreRepository<Void>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val nodeService: NodeService,
  @Autowired private val closureRetryService: ClosureRetryService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired
  private val paymentRequestInfoRedisTemplateWrapper: PaymentRequestInfoRedisTemplateWrapper,
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

  fun messageReceiver(
    queueEvent: Pair<TransactionUserCanceledEvent, TracingInfo?>,
    checkPointer: Checkpointer
  ) = messageReceiver(queueEvent, checkPointer, EmptyTransaction())

  fun messageReceiver(
    queueEvent: Pair<TransactionUserCanceledEvent, TracingInfo?>,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val (event, tracingInfo) = queueEvent
    val transactionId = event.transactionId
    val baseTransaction =
      reduceEvents(mono { transactionId }, transactionsEventStoreRepository, emptyTransaction)
    val closurePipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.CANCELLATION_REQUESTED) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = listOf(TransactionStatusDto.CANCELLATION_REQUESTED),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .cast(TransactionWithCancellationRequested::class.java)
        .flatMap { tx ->
          mono { nodeService.closePayment(tx.transactionId, ClosePaymentOutcome.KO) }
            .flatMap { closePaymentResponse ->
              updateTransactionStatus(
                transaction = tx, closePaymentResponseDto = closePaymentResponse)
            }
            .then()
            .onErrorResume { exception ->
              baseTransaction.flatMap { baseTransaction ->
                val enqueueRetryEvent =
                  when (exception) {
                    // close payment retry event enqueued only for 5XX http error codes
                    // or for any other exceptions that may happen during communication
                    // such as read timeout and so on
                    is ClosePaymentErrorResponseException ->
                      exception.statusCode == null || exception.statusCode.is5xxServerError
                    else -> {
                      true
                    }
                  }

                logger.error(
                  "Got error while calling closePaymentV2 for transaction with id ${tx.transactionId}! Enqueue retry event: $enqueueRetryEvent",
                  exception)

                if (enqueueRetryEvent) {
                  mono { baseTransaction }
                    .map { tx -> TransactionClosureErrorEvent(tx.transactionId.value().toString()) }
                    .flatMap { transactionClosureErrorEvent ->
                      transactionClosureErrorEventStoreRepository.save(transactionClosureErrorEvent)
                    }
                    .flatMap {
                      transactionsViewRepository.findByTransactionId(
                        baseTransaction.transactionId.value())
                    }
                    .cast(Transaction::class.java)
                    .flatMap { tx ->
                      tx.status = TransactionStatusDto.CLOSURE_ERROR
                      transactionsViewRepository.save(tx)
                    }
                    .flatMap {
                      reduceEvents(
                        mono { transactionId }, transactionsEventStoreRepository, emptyTransaction)
                    }
                    .flatMap { transactionUpdated ->
                      closureRetryService
                        .enqueueRetryEvent(transactionUpdated, 0, tracingInfo)
                        .doOnError(NoRetryAttemptsLeftException::class.java) { exception ->
                          logger.error("No more attempts left for closure retry", exception)
                        }
                    }
                } else {
                  Mono.empty()
                }
              }
            }
            .doFinally {
              tx.paymentNotices.forEach { el ->
                logger.info("Invalidate cache for RptId : {}", el.rptId().value())
                paymentRequestInfoRedisTemplateWrapper.deleteById(el.rptId().value())
              }
            }
        }
        .then()

    return if (tracingInfo != null) {
      runTracedPipelineWithDeadLetterQueue(
        checkPointer,
        closurePipeline,
        QueueEvent(event, tracingInfo),
        deadLetterTracedQueueAsyncClient,
        tracingUtils,
        this::class.simpleName!!)
    } else {
      runPipelineWithDeadLetterQueue(
        checkPointer,
        closurePipeline,
        BinaryData.fromObject(event).toBytes(),
        deadLetterTracedQueueAsyncClient,
      )
    }
  }

  private fun updateTransactionStatus(
    transaction: BaseTransactionWithCancellationRequested,
    closePaymentResponseDto: ClosePaymentResponseDto,
  ): Mono<TransactionClosedEvent> {
    val outcome =
      when (closePaymentResponseDto.outcome) {
        ClosePaymentResponseDto.OutcomeEnum.OK -> TransactionClosureData.Outcome.OK
        ClosePaymentResponseDto.OutcomeEnum.KO -> TransactionClosureData.Outcome.KO
      }

    val event =
      TransactionClosedEvent(transaction.transactionId.value(), TransactionClosureData(outcome))

    /*
     * the transaction was canceled by the user then it
     * will go to CANCELED status regardless the Nodo ClosePayment outcome
     */
    val newStatus = TransactionStatusDto.CANCELED

    logger.info(
      "Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

    val transactionUpdate =
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)

    return transactionClosureSentEventRepository.save(event).flatMap { closedEvent ->
      transactionUpdate
        .flatMap { tx ->
          tx.status = newStatus
          transactionsViewRepository.save(tx)
        }
        .thenReturn(closedEvent)
    }
  }
}
