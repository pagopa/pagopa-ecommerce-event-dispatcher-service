package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureRequested
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithClosureRequested
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithCompletedAuthorization
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.redis.templatewrappers.PaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadClosePaymentRequest
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.ecommerce.eventdispatcher.queues.v2.reduceEvents
import it.pagopa.ecommerce.eventdispatcher.queues.v2.refundTransaction
import it.pagopa.ecommerce.eventdispatcher.queues.v2.runTracedPipelineWithDeadLetterQueue
import it.pagopa.ecommerce.eventdispatcher.queues.v2.updateTransactionToRefundRequested
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NodeService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto.OutcomeEnum
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

data class ClosePaymentTransactionData(
  val closureOutcome: OutcomeEnum,
  val wasAuthorized: Boolean,
  val canceledByUser: Boolean
)

data class ClosePaymentEvent(
  val requested: QueueEvent<TransactionClosureRequestedEvent>?,
  val canceled: QueueEvent<TransactionUserCanceledEvent>?,
  val retried: QueueEvent<TransactionClosureRetriedEvent>?,
  val errored: QueueEvent<TransactionClosureErrorEvent>?
) {
  init {
    require(listOfNotNull(requested, canceled, retried, errored).size == 1) {
      "Only one event must be non-null!"
    }
  }

  companion object {
    fun requested(event: QueueEvent<TransactionClosureRequestedEvent>): ClosePaymentEvent =
      ClosePaymentEvent(event, null, null, null)

    fun canceled(event: QueueEvent<TransactionUserCanceledEvent>): ClosePaymentEvent =
      ClosePaymentEvent(null, event, null, null)

    fun retried(event: QueueEvent<TransactionClosureRetriedEvent>): ClosePaymentEvent =
      ClosePaymentEvent(null, null, event, null)

    fun errored(event: QueueEvent<TransactionClosureErrorEvent>): ClosePaymentEvent =
      ClosePaymentEvent(null, null, null, event)
  }

  fun <T> fold(
    onClosureRequested: (QueueEvent<TransactionClosureRequestedEvent>) -> T,
    onCanceled: (QueueEvent<TransactionUserCanceledEvent>) -> T,
    onRetried: (QueueEvent<TransactionClosureRetriedEvent>) -> T,
    onErrored: (QueueEvent<TransactionClosureErrorEvent>) -> T
  ): T {
    return if (requested != null) {
      onClosureRequested(requested)
    } else if (canceled != null) {
      onCanceled(canceled)
    } else if (retried != null) {
      onRetried(retried)
    } else if (errored != null) {
      onErrored(errored)
    } else {
      throw IllegalStateException("No variant of `ClosePaymentEvent` is non-null!")
    }
  }
}

/**
 * This helper implements the business logic related to handling calling `closePaymentV2`. In
 * particular, the [closePayment] method does the following:
 * - checks for the transaction current status
 * - determines whether the transaction was canceled by the user, whether it was authorized and what
 * the outcome to be sent to the `closePaymentV2` should be
 * - calls Nodo's `closePaymentV2`
 * - triggers an immediate refund if the transaction was authorized and Nodo responded with a KO
 * outcome
 * - enqueues a retry event in case of error
 * - flushes eCommerce activation cache
 */
@Component
class ClosePaymentHelper(
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionClosureSentEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData>,
  @Autowired
  private val transactionClosureErrorEventStoreRepository: TransactionsEventStoreRepository<Void>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val nodeService: NodeService,
  @Autowired private val closureRetryService: ClosureRetryService,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val refundService: RefundService,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired
  private val paymentRequestInfoRedisTemplateWrapper: PaymentRequestInfoRedisTemplateWrapper,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
) {
  val logger: Logger = LoggerFactory.getLogger(ClosePaymentHelper::class.java)

  val closureRequestedValidStatuses =
    setOf(
      TransactionStatusDto.CANCELLATION_REQUESTED,
      TransactionStatusDto.CLOSURE_REQUESTED,
      TransactionStatusDto.CLOSURE_ERROR)

  fun closePayment(
    queueEvent: ClosePaymentEvent,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val tracingInfo = getTracingInfo(queueEvent)
    val transactionId = getTransactionId(queueEvent)
    val retryCount = getRetryCount(queueEvent)
    val baseTransaction =
      reduceEvents(mono { transactionId }, transactionsEventStoreRepository, emptyTransaction)
    val closurePipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (!closureRequestedValidStatuses.contains(it.status)) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = closureRequestedValidStatuses.toList(),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .flatMap { tx ->
          val closePaymentTransactionData =
            when (tx) {
              is TransactionWithClosureError -> Mono.just(getClosePaymentTransactionData(tx))
              is TransactionWithCancellationRequested ->
                Mono.just(getClosePaymentTransactionData(tx))
              is TransactionWithClosureRequested -> Mono.just(getClosePaymentTransactionData(tx))
              else ->
                Mono.error(
                  IllegalArgumentException(
                    "Invalid transaction type! Decoded type is ${tx.javaClass}"))
            }

          closePaymentTransactionData.map { Pair(tx, it) }
        }
        .flatMap { (tx, closePaymentTransactionData) ->
          mono {
              nodeService.closePayment(tx.transactionId, closePaymentTransactionData.closureOutcome)
            }
            .flatMap { closePaymentResponse ->
              updateTransactionStatus(
                transaction = tx,
                closePaymentResponseDto = closePaymentResponse,
                closePaymentTransactionData = closePaymentTransactionData)
            }
            /*
             * The refund process is started only iff the previous transaction was authorized
             * and the Nodo returned closePaymentV2 response outcome KO
             */
            .flatMap { closePaymentOutcomeEvent ->
              closePaymentOutcomeEvent.fold(
                { Mono.empty() },
                { transactionClosedEvent ->
                  when (tx) {
                    is TransactionWithClosureRequested ->
                      refundTransactionPipeline(
                        Either.right(tx), transactionClosedEvent.data.responseOutcome, tracingInfo)
                    is TransactionWithClosureError ->
                      refundTransactionPipeline(
                        Either.left(tx), transactionClosedEvent.data.responseOutcome, tracingInfo)
                    is TransactionWithCancellationRequested -> Mono.empty()
                    else ->
                      Mono.error(
                        IllegalArgumentException(
                          "Invalid transaction type! Decoded type is ${tx.javaClass}"))
                  }
                })
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
                          .enqueueRetryEvent(transactionUpdated, retryCount, tracingInfo)
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

    val event = queueEvent.fold({ it }, { it }, { it }, { it })
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      closurePipeline,
      event,
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  private fun updateTransactionStatus(
    transaction: BaseTransaction,
    closePaymentTransactionData: ClosePaymentTransactionData,
    closePaymentResponseDto: ClosePaymentResponseDto
  ): Mono<Either<TransactionClosureFailedEvent, TransactionClosedEvent>> {
    val outcome =
      when (closePaymentResponseDto.outcome) {
        ClosePaymentResponseDto.OutcomeEnum.OK -> TransactionClosureData.Outcome.OK
        ClosePaymentResponseDto.OutcomeEnum.KO -> TransactionClosureData.Outcome.KO
      }

    val wasAuthorized = closePaymentTransactionData.wasAuthorized
    val canceledByUser = closePaymentTransactionData.canceledByUser

    val event: Either<TransactionClosureFailedEvent, TransactionClosedEvent> =
      if (!wasAuthorized && !canceledByUser) {
        Either.left(
          TransactionClosureFailedEvent(
            transaction.transactionId.value(), TransactionClosureData(outcome)))
      } else {
        Either.right(
          TransactionClosedEvent(
            transaction.transactionId.value(), TransactionClosureData(outcome)))
      }

    /*
     * if the transaction was canceled by the user the transaction
     * will go to CANCELED status regardless the Nodo ClosePayment outcome
     */
    val newStatus =
      if (canceledByUser) {
        TransactionStatusDto.CANCELED
      } else {
        when (closePaymentTransactionData.closureOutcome) {
          OutcomeEnum.OK -> TransactionStatusDto.CLOSED
          OutcomeEnum.KO -> TransactionStatusDto.UNAUTHORIZED
        }
      }

    logger.info(
      "Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

    val transactionUpdate =
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)

    val saveEvent =
      event.bimap(
        {
          transactionClosureSentEventRepository.save(it).flatMap { closedEvent ->
            transactionUpdate
              .flatMap { tx ->
                tx.status = newStatus
                transactionsViewRepository.save(tx)
              }
              .thenReturn(closedEvent)
          }
        },
        {
          transactionClosureSentEventRepository.save(it).flatMap { closedEvent ->
            transactionUpdate
              .flatMap { tx ->
                tx.status = newStatus
                transactionsViewRepository.save(tx)
              }
              .thenReturn(closedEvent)
          }
        })

    return saveEvent.fold(
      { it.map { closureFailed -> Either.left(closureFailed) } },
      { it.map { closed -> Either.right(closed) } })
  }

  private fun wasTransactionCanceledByUser(
    transactionAtPreviousState:
      Optional<
        Either<BaseTransactionWithCancellationRequested, BaseTransactionWithClosureRequested>>
  ): Boolean = transactionAtPreviousState.map { it.isLeft }.orElse(false)

  private fun wasTransactionAuthorized(
    transactionAtPreviousState:
      Optional<
        Either<BaseTransactionWithCancellationRequested, BaseTransactionWithClosureRequested>>
  ): Boolean =
    transactionAtPreviousState
      .map { it.fold({ false }, { tx -> wasTransactionAuthorized(tx) }) }
      .orElseGet { false }

  private fun wasTransactionAuthorized(transaction: BaseTransactionWithClosureRequested): Boolean {
    val transactionGatewayData =
      transaction.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
    return when (transactionGatewayData) {
      is PgsTransactionGatewayAuthorizationData ->
        transactionGatewayData.authorizationResultDto == AuthorizationResultDto.OK
      is NpgTransactionGatewayAuthorizationData ->
        transactionGatewayData.operationResult == OperationResultDto.EXECUTED
      is RedirectTransactionGatewayAuthorizationData -> TODO()
    }
  }

  private fun getBaseTransactionWithCompletedAuthorization(
    transactionAtPreviousState:
      Optional<
        Either<BaseTransactionWithCancellationRequested, BaseTransactionWithClosureRequested>>
  ): Optional<BaseTransactionWithCompletedAuthorization> =
    transactionAtPreviousState.flatMap { either ->
      either.fold({ Optional.empty() }, { Optional.of(it) })
    }

  private fun getClosePaymentTransactionData(
    transaction: TransactionWithClosureError
  ): ClosePaymentTransactionData {
    return ClosePaymentTransactionData(
      closureOutcome = getClosePaymentOutcome(transaction),
      canceledByUser = wasTransactionCanceledByUser(transaction.transactionAtPreviousState()),
      wasAuthorized = wasTransactionAuthorized(transaction.transactionAtPreviousState()))
  }

  private fun getClosePaymentTransactionData(
    transaction: BaseTransactionWithCancellationRequested
  ): ClosePaymentTransactionData {
    return ClosePaymentTransactionData(
      closureOutcome = getClosePaymentOutcome(transaction),
      canceledByUser = true,
      wasAuthorized = false)
  }

  private fun getClosePaymentTransactionData(
    transaction: TransactionWithClosureRequested
  ): ClosePaymentTransactionData {
    return ClosePaymentTransactionData(
      closureOutcome = getClosePaymentOutcome(transaction),
      canceledByUser = false,
      wasAuthorized = wasTransactionAuthorized(transaction))
  }

  private fun getClosePaymentOutcome(transaction: TransactionWithClosureError): OutcomeEnum {
    val transactionAtPreviousState = transaction.transactionAtPreviousState()

    val closureOutcome =
      transactionAtPreviousState
        .map {
          it.fold(
            { transactionWithCancellationRequested ->
              getClosePaymentOutcome(transactionWithCancellationRequested)
            },
            { trxWithAuthorizationCompleted ->
              getClosePaymentOutcome(trxWithAuthorizationCompleted)
            })
        }
        .orElseThrow {
          RuntimeException(
            "Unexpected transactionAtPreviousStep: ${transaction.transactionAtPreviousState}")
        }

    return closureOutcome
  }

  private fun getClosePaymentOutcome(
    transaction: BaseTransactionWithClosureRequested
  ): OutcomeEnum {
    /*
     * retrying a close payment for an authorized transaction.
     * Will be performed a close payment OK/KO based on the authorization outcome
     */

    val transactionAuthGatewayData =
      transaction.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData

    val closureOutcome =
      when (transactionAuthGatewayData) {
        is PgsTransactionGatewayAuthorizationData ->
          when (transactionAuthGatewayData.authorizationResultDto) {
            AuthorizationResultDto.OK -> OutcomeEnum.OK
            else -> OutcomeEnum.KO
          }
        is NpgTransactionGatewayAuthorizationData ->
          when (transactionAuthGatewayData.operationResult) {
            OperationResultDto.EXECUTED -> OutcomeEnum.OK
            else -> OutcomeEnum.KO
          }
        is RedirectTransactionGatewayAuthorizationData -> TODO()
      }

    return closureOutcome
  }

  private fun getClosePaymentOutcome(
    ignored: BaseTransactionWithCancellationRequested
  ): OutcomeEnum {
    /*
     * retrying a closure for a transaction canceled by the user (not authorized) so here
     * we have to perform a closePayment KO request to Nodo
     */
    return OutcomeEnum.KO
  }

  private fun refundTransactionPipeline(
    transaction: Either<TransactionWithClosureError, TransactionWithClosureRequested>,
    closureOutcome: TransactionClosureData.Outcome,
    tracingInfo: TracingInfo
  ): Mono<BaseTransaction> {
    val (wasAuthorized, transactionWithCompletedAuthorization) =
      transaction.fold(
        {
          val transactionAtPreviousState = it.transactionAtPreviousState()
          val wasAuthorized = wasTransactionAuthorized(transactionAtPreviousState)
          val transactionWithCompletedAuthorization =
            getBaseTransactionWithCompletedAuthorization(transactionAtPreviousState)

          Pair(wasAuthorized, transactionWithCompletedAuthorization)
        },
        {
          val wasAuthorized = wasTransactionAuthorized(it)
          val transactionWithCompletedAuthorization = Optional.of(it)

          Pair(wasAuthorized, transactionWithCompletedAuthorization)
        })
    val toBeRefunded = wasAuthorized && closureOutcome == TransactionClosureData.Outcome.KO
    logger.info(
      "Transaction Nodo ClosePaymentV2 response outcome: $closureOutcome, was authorized: $wasAuthorized --> to be refunded: $toBeRefunded")

    return Mono.just(transactionWithCompletedAuthorization)
      .filter { it.isPresent && toBeRefunded }
      .flatMap { tx ->
        updateTransactionToRefundRequested(
          tx.get(), transactionsRefundedEventStoreRepository, transactionsViewRepository)
      }
      .flatMap {
        refundTransaction(
          transactionWithCompletedAuthorization.get(),
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          paymentGatewayClient,
          refundService,
          refundRetryService,
          tracingInfo)
      }
  }

  private fun getTracingInfo(event: ClosePaymentEvent): TracingInfo {
    return event.fold(
      { it.tracingInfo }, { it.tracingInfo }, { it.tracingInfo }, { it.tracingInfo })
  }

  private fun getTransactionId(event: ClosePaymentEvent): String {
    return event.fold(
      { it.event.transactionId },
      { it.event.transactionId },
      { it.event.transactionId },
      { it.event.transactionId })
  }

  private fun getRetryCount(event: ClosePaymentEvent): Int {
    return event.fold({ 0 }, { 0 }, { it.event.data.retryCount }, { 0 })
  }
}
