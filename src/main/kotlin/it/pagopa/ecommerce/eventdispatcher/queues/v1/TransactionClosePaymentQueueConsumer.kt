package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
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
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v1.NodeService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionClosePaymentQueueConsumerV1")
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
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
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
          mono {
              nodeService.closePayment(tx.transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO)
            }
            .flatMap { closePaymentResponse ->
              updateTransactionStatus(
                transaction = tx, closePaymentResponseDto = closePaymentResponse)
            }
            .then()
            .onErrorResume { exception ->
              baseTransaction.flatMap { baseTransaction ->
                when (exception) {
                  is BadClosePaymentRequest ->
                    mono { baseTransaction }
                      .flatMap {
                        logger.error(
                          "Got unrecoverable error (400 - Bad Request) while calling closePaymentV2 for transaction with id ${it.transactionId}!",
                          exception)
                        Mono.empty()
                      }
                  is TransactionNotFound ->
                    mono { baseTransaction }
                      .flatMap {
                        logger.error(
                          "Got unrecoverable error (404 - Not Founds) while calling closePaymentV2 for transaction with id ${it.transactionId}!",
                          exception)
                        Mono.empty()
                      }
                  else -> {
                    logger.error(
                      "Got exception while calling closePaymentV2 for transaction with id ${baseTransaction.transactionId}!",
                      exception)

                    mono { baseTransaction }
                      .map { tx ->
                        TransactionClosureErrorEvent(tx.transactionId.value().toString())
                      }
                      .flatMap { transactionClosureErrorEvent ->
                        transactionClosureErrorEventStoreRepository.save(
                          transactionClosureErrorEvent)
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
                          mono { transactionId },
                          transactionsEventStoreRepository,
                          emptyTransaction)
                      }
                      .flatMap { transactionUpdated ->
                        closureRetryService
                          .enqueueRetryEvent(transactionUpdated, 0, tracingInfo)
                          .doOnError(NoRetryAttemptsLeftException::class.java) { exception ->
                            logger.error("No more attempts left for closure retry", exception)
                          }
                      }
                  }
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
        deadLetterQueueAsyncClient,
        deadLetterTTLSeconds,
        tracingUtils,
        this::class.simpleName!!)
    } else {
      runPipelineWithDeadLetterQueue(
        checkPointer,
        closurePipeline,
        BinaryData.fromObject(event).toBytes(),
        deadLetterQueueAsyncClient,
        deadLetterTTLSeconds,
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
