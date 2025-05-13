package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationRequestedData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.*
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.redis.templatewrappers.v2.PaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils.UpdateTransactionStatusOutcome
import it.pagopa.ecommerce.commons.utils.UpdateTransactionStatusTracerUtils.UserCancelClosePaymentNodoStatusUpdate
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentEvent.Companion.exceptionToClosureErrorData
import it.pagopa.ecommerce.eventdispatcher.queues.v2.reduceEvents
import it.pagopa.ecommerce.eventdispatcher.queues.v2.requestRefundTransaction
import it.pagopa.ecommerce.eventdispatcher.queues.v2.runTracedPipelineWithDeadLetterQueue
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NodeService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.utils.TransactionTracing
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.Duration
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

data class ClosePaymentTransactionData(
  val closureOutcome: ClosePaymentOutcome,
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

    fun exceptionToClosureErrorData(exception: Throwable): ClosureErrorData =
      if (exception is ClosePaymentErrorResponseException) {
        ClosureErrorData(
          exception.statusCode,
          exception.errorResponse?.description,
          if (exception.statusCode != null) {
            ClosureErrorData.ErrorType.KO_RESPONSE_RECEIVED
          } else {
            ClosureErrorData.ErrorType.COMMUNICATION_ERROR
          })
      } else {
        ClosureErrorData(null, null, ClosureErrorData.ErrorType.COMMUNICATION_ERROR)
      }
  }

  fun <T> fold(
    onClosureRequested: (QueueEvent<TransactionClosureRequestedEvent>) -> T,
    onCanceled: (QueueEvent<TransactionUserCanceledEvent>) -> T,
    onRetried: (QueueEvent<TransactionClosureRetriedEvent>) -> T,
    onErrored: (QueueEvent<TransactionClosureErrorEvent>) -> T
  ): T {
    return checkNotNull(
      when {
        requested != null -> onClosureRequested(requested)
        canceled != null -> onCanceled(canceled)
        retried != null -> onRetried(retried)
        errored != null -> onErrored(errored)
        else -> null
      }) { "No variant of `ClosePaymentEvent` is non-null!" }
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
  private val transactionClosureErrorEventStoreRepository:
    TransactionsEventStoreRepository<ClosureErrorData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val nodeService: NodeService,
  @Autowired private val closureRetryService: ClosureRetryService,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired
  private val paymentRequestInfoRedisTemplateWrapper: PaymentRequestInfoRedisTemplateWrapper,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val npgService: NpgService,
  @Autowired private val refundQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
  private val transientQueueTTLSeconds: Int,
  @Autowired private val updateTransactionStatusTracerUtils: UpdateTransactionStatusTracerUtils,
  @Autowired private val transactionTracing: TransactionTracing
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

    val events =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
        .map { it as TransactionEvent<Any> }

    val baseTransaction = reduceEvents(events, emptyTransaction)

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
                Mono.just(closePaymentTransactionDataForTransactionCanceledByUser)
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
            .doFinally {
              if (closePaymentTransactionData.canceledByUser) {
                tx.paymentNotices.forEach { el ->
                  logger.info("Invalidate cache for RptId : {}", el.rptId().value())
                  paymentRequestInfoRedisTemplateWrapper.deleteById(el.rptId().value())
                }
              }
            }
            .flatMap { closePaymentResponse ->
              updateTransactionStatus(
                transaction = tx,
                closePaymentResponseDto = closePaymentResponse,
                closePaymentTransactionData = closePaymentTransactionData,
                events = events)
            }
            /*
             * The refund process is started only iff the previous transaction was authorized
             * and the Nodo returned closePaymentV2 response outcome KO
             */
            .flatMap { closePaymentOutcomeEvent ->
              closePaymentOutcomeEvent.fold(
                { Mono.empty() },
                { transactionClosedEvent ->
                  requestRefundTransactionPipeline(
                    (tx as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(
                      transactionClosedEvent) as BaseTransaction,
                    transactionClosedEvent.data.responseOutcome,
                    tracingInfo)
                })
            }
            .then()
            .onErrorResume {
              closePaymentErrorHandling(
                exception = it,
                baseTransaction = baseTransaction,
                retryCount = retryCount,
                tracingInfo = tracingInfo,
                closePaymentTransactionData = closePaymentTransactionData)
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

  private fun closePaymentErrorHandling(
    exception: Throwable,
    baseTransaction: Mono<BaseTransaction>,
    retryCount: Int,
    tracingInfo: TracingInfo,
    closePaymentTransactionData: ClosePaymentTransactionData
  ) =
    baseTransaction
      .publishOn(Schedulers.boundedElastic())
      .flatMap { tx ->
        logger.error(
          "Got exception while processing closePaymentV2 for transaction with id ${tx.transactionId.value()}!",
          exception)

        updateTransactionToClosureError(tx, exception)
      }
      .flatMap { tx ->
        val (statusCode, errorDescription, refundTransaction) =
          if (exception is ClosePaymentErrorResponseException) {
            Triple(
              exception.statusCode,
              exception.errorResponse?.description,
              exception.isRefundableError())
          } else {
            Triple(null, null, false)
          }
        traceClosePaymentUpdateStatus(
          baseTransaction = tx,
          closePaymentTransactionData = closePaymentTransactionData,
          nodeResult =
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              ClosePaymentOutcome.KO.toString(),
              Optional.of(
                "HTTP code:[${statusCode?.value() ?: "N/A"}] - descr:[${errorDescription ?: "N/A"}]"),
            ),
          updateTransactionStatusOutcome = UpdateTransactionStatusOutcome.PROCESSING_ERROR)

        // retry event enqueued only for 5xx error responses or for other exceptions that might
        // happen
        // during communication such as read timeout
        val enqueueRetryEvent =
          !refundTransaction && (statusCode == null || statusCode.is5xxServerError)
        logger.info(
          "Handling Nodo close payment error response. Status code: [{}], error description: [{}] -> refund transaction: [{}], enqueue retry event: [{}]",
          statusCode,
          errorDescription,
          refundTransaction,
          enqueueRetryEvent)
        if (refundTransaction) {
          requestRefundTransactionPipeline(tx, TransactionClosureData.Outcome.KO, tracingInfo)
            .then()
        } else {
          if (enqueueRetryEvent) {
            enqueueClosureRetryEventPipeline(
              baseTransaction = tx,
              retryCount = retryCount,
              tracingInfo = tracingInfo,
              exception = exception)
          } else {
            Mono.empty()
          }
        }
      }

  private fun enqueueClosureRetryEventPipeline(
    baseTransaction: BaseTransaction,
    retryCount: Int,
    tracingInfo: TracingInfo,
    exception: Throwable
  ) =
    closureRetryService
      .enqueueRetryEvent(
        baseTransaction = baseTransaction,
        retriedCount = retryCount,
        tracingInfo = tracingInfo,
        throwable = exception)
      .publishOn(Schedulers.boundedElastic())
      .doOnError(NoRetryAttemptsLeftException::class.java) {
        logger.error("No more attempts left for closure retry", it)
      }

  private fun updateTransactionToClosureError(
    baseTransaction: BaseTransaction,
    exception: Throwable
  ): Mono<BaseTransactionWithClosureError> {
    val closureErrorData = exceptionToClosureErrorData(exception)
    return if (baseTransaction.status != TransactionStatusDto.CLOSURE_ERROR) {
      logger.info(
        "Updating transaction with id: [${baseTransaction.transactionId.value()}] to ${TransactionStatusDto.CLOSURE_ERROR} status")
      val event =
        TransactionClosureErrorEvent(baseTransaction.transactionId.value(), closureErrorData)

      transactionClosureErrorEventStoreRepository
        .save(event)
        .flatMap {
          transactionsViewRepository.findByTransactionId(baseTransaction.transactionId.value())
        }
        .cast(Transaction::class.java)
        .flatMap { trx ->
          trx.status = TransactionStatusDto.CLOSURE_ERROR
          trx.closureErrorData = closureErrorData
          transactionsViewRepository.save(trx)
        }
        .thenReturn(
          (baseTransaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(event)
            as BaseTransactionWithClosureError)
    } else {
      logger.info(
        "Transaction with id: [${baseTransaction.transactionId.value()}] already in ${TransactionStatusDto.CLOSURE_ERROR} status")
      transactionsViewRepository
        .findByTransactionId(baseTransaction.transactionId.value())
        .cast(Transaction::class.java)
        .flatMap { trx ->
          trx.status = TransactionStatusDto.CLOSURE_ERROR
          trx.closureErrorData = closureErrorData
          transactionsViewRepository.save(trx)
        }
        .thenReturn((baseTransaction as BaseTransactionWithClosureError))
    }
  }

  private fun updateTransactionStatus(
    transaction: BaseTransaction,
    closePaymentTransactionData: ClosePaymentTransactionData,
    closePaymentResponseDto: ClosePaymentResponseDto,
    events: Flux<TransactionEvent<Any>>
  ): Mono<Either<TransactionClosureFailedEvent, TransactionClosedEvent>> {
    val outcome =
      when (closePaymentResponseDto.outcome!!) {
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
          ClosePaymentOutcome.OK -> TransactionStatusDto.CLOSED
          ClosePaymentOutcome.KO -> TransactionStatusDto.UNAUTHORIZED
        }
      }

    logger.info(
      "Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

    val transactionUpdate =
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)

    val sendPaymentResultOutcome =
      if (!canceledByUser && closePaymentTransactionData.closureOutcome == ClosePaymentOutcome.OK) {
        TransactionUserReceiptData.Outcome.NOT_RECEIVED
      } else {
        null
      }

    val saveEvent =
      event.bimap(
        {
          transactionClosureSentEventRepository.save(it).flatMap { closedEvent ->
            transactionUpdate
              .flatMap { tx ->
                tx.status = newStatus
                tx.sendPaymentResultOutcome = sendPaymentResultOutcome
                tx.closureErrorData =
                  null // reset closure error state when a close payment response have been received
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
                tx.sendPaymentResultOutcome = sendPaymentResultOutcome
                tx.closureErrorData =
                  null // reset closure error state when a close payment response have been received
                transactionsViewRepository.save(tx)
              }
              .thenReturn(closedEvent)
          }
        })

    return saveEvent
      .fold<Mono<Either<TransactionClosureFailedEvent, TransactionClosedEvent>>>(
        { it.map { closureFailed -> Either.left(closureFailed) } },
        { it.map { closed -> Either.right(closed) } })
      .doOnSuccess { result ->
        traceClosePaymentUpdateStatus(
          baseTransaction = transaction,
          closePaymentTransactionData = closePaymentTransactionData,
          nodeResult =
            UpdateTransactionStatusTracerUtils.GatewayOutcomeResult(
              outcome.toString(), Optional.empty()),
          updateTransactionStatusOutcome = UpdateTransactionStatusOutcome.OK)

        if (newStatus == TransactionStatusDto.CANCELED ||
          newStatus == TransactionStatusDto.UNAUTHORIZED) {
          val updatedTransaction =
            result.fold(
              { closureFailedEvent ->
                (transaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(
                  closureFailedEvent) as BaseTransaction
              },
              { closedEvent ->
                (transaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(
                  closedEvent) as BaseTransaction
              })
          transactionTracing.addSpanAttributesCanceledOrUnauthorizedFlowFromTransaction(
            updatedTransaction, events)
        }
      }
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
      is NpgTransactionGatewayAuthorizationData ->
        transactionGatewayData.operationResult == OperationResultDto.EXECUTED
      is RedirectTransactionGatewayAuthorizationData ->
        transactionGatewayData.outcome == RedirectTransactionGatewayAuthorizationData.Outcome.OK
      is PgsTransactionGatewayAuthorizationData ->
        throw IllegalArgumentException(
          "Unhandled or invalid auth data type 'PgsTransactionGatewayAuthorizationData'")
    }
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
    transaction: TransactionWithClosureRequested
  ): ClosePaymentTransactionData {
    return ClosePaymentTransactionData(
      closureOutcome = getClosePaymentOutcome(transaction),
      canceledByUser = false,
      wasAuthorized = wasTransactionAuthorized(transaction))
  }

  val closePaymentTransactionDataForTransactionCanceledByUser =
    ClosePaymentTransactionData(
      closureOutcome = ClosePaymentOutcome.KO, canceledByUser = true, wasAuthorized = false)

  private fun getClosePaymentOutcome(
    transaction: TransactionWithClosureError
  ): ClosePaymentOutcome {
    val transactionAtPreviousState = transaction.transactionAtPreviousState()

    val closureOutcome =
      transactionAtPreviousState
        .map {
          it.fold(
            { _ ->
              /*
               * retrying a closure for a transaction canceled by the user (not authorized) so here
               * we have to perform a closePayment KO request to Nodo
               */
              ClosePaymentOutcome.KO
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
  ): ClosePaymentOutcome {
    /*
     * retrying a close payment for an authorized transaction.
     * Will be performed a close payment OK/KO based on the authorization outcome
     */

    val transactionAuthGatewayData =
      transaction.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData

    val closureOutcome =
      when (transactionAuthGatewayData) {
        is NpgTransactionGatewayAuthorizationData ->
          when (transactionAuthGatewayData.operationResult) {
            OperationResultDto.EXECUTED -> ClosePaymentOutcome.OK
            else -> ClosePaymentOutcome.KO
          }
        is RedirectTransactionGatewayAuthorizationData ->
          when (transactionAuthGatewayData.outcome) {
            RedirectTransactionGatewayAuthorizationData.Outcome.OK -> ClosePaymentOutcome.OK
            else -> ClosePaymentOutcome.KO
          }
        is PgsTransactionGatewayAuthorizationData ->
          throw IllegalArgumentException(
            "Unhandled or invalid auth data type 'PgsTransactionGatewayAuthorizationData'")
      }

    return closureOutcome
  }

  private fun requestRefundTransactionPipeline(
    transaction: BaseTransaction,
    closureOutcome: TransactionClosureData.Outcome,
    tracingInfo: TracingInfo
  ): Mono<BaseTransaction> =
    when (transaction) {
      is TransactionClosed ->
        requestRefundTransactionPipeline(Either.right(transaction), closureOutcome, tracingInfo)
      is TransactionWithClosureError ->
        requestRefundTransactionPipeline(Either.left(transaction), closureOutcome, tracingInfo)
      else -> Mono.empty()
    }

  private fun requestRefundTransactionPipeline(
    transaction: Either<TransactionWithClosureError, TransactionClosed>,
    closureOutcome: TransactionClosureData.Outcome,
    tracingInfo: TracingInfo
  ): Mono<BaseTransaction> {
    val (wasAuthorized, transactionWithCompletedAuthorization) =
      transaction.fold(
        {
          val wasAuthorized = wasTransactionAuthorized(it.transactionAtPreviousState())

          Pair(wasAuthorized, it)
        },
        {
          val wasAuthorized = wasTransactionAuthorized(it)

          Pair(wasAuthorized, it)
        })
    val toBeRefunded = wasAuthorized && closureOutcome == TransactionClosureData.Outcome.KO
    logger.info(
      "Transaction Nodo ClosePaymentV2 response outcome: $closureOutcome, was authorized: $wasAuthorized --> to be refunded: $toBeRefunded")

    return Mono.just(transactionWithCompletedAuthorization)
      .filter { toBeRefunded }
      .flatMap {
        requestRefundTransaction(
          it,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          npgService,
          tracingInfo,
          refundQueueAsyncClient,
          Duration.ofSeconds(transientQueueTTLSeconds.toLong()))
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

  private fun traceClosePaymentUpdateStatus(
    baseTransaction: BaseTransaction,
    closePaymentTransactionData: ClosePaymentTransactionData,
    nodeResult: UpdateTransactionStatusTracerUtils.GatewayOutcomeResult,
    updateTransactionStatusOutcome: UpdateTransactionStatusOutcome
  ) {
    val statusUpdateInfo =
      if (closePaymentTransactionData.canceledByUser) {
        UserCancelClosePaymentNodoStatusUpdate(
          updateTransactionStatusOutcome, baseTransaction.clientId, nodeResult)
      } else {
        val baseTransactionWithRequestedAuthorization =
          baseTransaction.let {
            if (it is BaseTransactionWithClosureError) {
              it.transactionAtPreviousState
            } else {
              it
            }
            // safe cast here: close payment is performed for user canceled transactions or
            // transactions for which have been requested authorization
          } as BaseTransactionWithRequestedAuthorization
        UpdateTransactionStatusTracerUtils.ClosePaymentNodoStatusUpdate(
          updateTransactionStatusOutcome,
          baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData.pspId,
          baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
            .paymentTypeCode,
          baseTransactionWithRequestedAuthorization.clientId,
          Optional.of(
              baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
                .transactionGatewayAuthorizationRequestedData)
            .filter { it is NpgTransactionGatewayAuthorizationRequestedData }
            .map { it as NpgTransactionGatewayAuthorizationRequestedData }
            .map { it.walletInfo != null }
            .orElse(false),
          nodeResult)
      }
    updateTransactionStatusTracerUtils.traceStatusUpdateOperation(statusUpdateInfo)
  }
}
