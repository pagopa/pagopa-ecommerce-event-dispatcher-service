package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCompletedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.NodeClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.ClosePaymentErrorResponseException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v1.NodeService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionClosePaymentRetryQueueConsumerV1")
@Deprecated("Mark for deprecation in favor of V2 version")
class TransactionClosePaymentRetryQueueConsumer(
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionClosureSentEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val nodeService: NodeService,
  @Autowired private val closureRetryService: ClosureRetryService,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils
) {
  var logger: Logger =
    LoggerFactory.getLogger(TransactionClosePaymentRetryQueueConsumer::class.java)

  private fun getTransactionId(
    event: Either<TransactionClosureErrorEvent, TransactionClosureRetriedEvent>
  ): String {
    return event.fold({ it.transactionId }, { it.transactionId })
  }

  private fun getRetryCount(
    event: Either<TransactionClosureErrorEvent, TransactionClosureRetriedEvent>
  ): Int {
    return event.fold({ 0 }, { it.data.retryCount })
  }

  fun messageReceiver(
    parsedEvent:
      Pair<Either<TransactionClosureErrorEvent, TransactionClosureRetriedEvent>, TracingInfo?>,
    checkPointer: Checkpointer
  ) = messageReceiver(parsedEvent, checkPointer, EmptyTransaction())

  fun messageReceiver(
    parsedEvent:
      Pair<Either<TransactionClosureErrorEvent, TransactionClosureRetriedEvent>, TracingInfo?>,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val (event, tracingInfo) = parsedEvent
    val transactionId = getTransactionId(event)
    val retryCount = getRetryCount(event)
    val baseTransaction =
      reduceEvents(mono { transactionId }, transactionsEventStoreRepository, emptyTransaction)
    val closurePipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.CLOSURE_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = listOf(TransactionStatusDto.CLOSURE_ERROR),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .cast(TransactionWithClosureError::class.java)
        .flatMap { tx ->
          val transactionAtPreviousState = tx.transactionAtPreviousState()
          val canceledByUser = wasTransactionCanceledByUser(transactionAtPreviousState)
          val wasAuthorized = wasTransactionAuthorized(transactionAtPreviousState)
          val closureOutcome =
            tx
              .transactionAtPreviousState()
              .map {
                it.fold(
                  {
                    /*
                     * retrying a closure for a transaction canceled by the user (not authorized) so here
                     * we have to perform a closePayment KO request to Nodo
                     */
                    ClosePaymentOutcome.KO
                  },
                  {
                    /*
                     * retrying a close payment for an authorized transaction.
                     * Will be performed a close payment OK/KO based on the authorization outcome
                     */
                    trxWithAuthorizationCompleted ->
                    when (trxWithAuthorizationCompleted.transactionAuthorizationCompletedData
                      .authorizationResultDto) {
                      AuthorizationResultDto.OK -> ClosePaymentOutcome.OK
                      AuthorizationResultDto.KO -> ClosePaymentOutcome.KO
                      else ->
                        throw RuntimeException(
                          "authorizationResult in status update event is null!")
                    }
                  })
              }
              .orElseThrow {
                RuntimeException(
                  "Unexpected transactionAtPreviousStep: ${tx.transactionAtPreviousState}")
              }

          mono { nodeService.closePayment(tx.transactionId, closureOutcome) }
            .flatMap { closePaymentResponse ->
              updateTransactionStatus(
                transaction = tx,
                closureOutcome = closureOutcome,
                closePaymentResponseDto = closePaymentResponse,
                canceledByUser = canceledByUser,
                wasAuthorized = wasAuthorized)
            }
            /*
             * The refund process is started only iff the previous transaction was authorized
             * and the Nodo returned closePaymentV2 response outcome KO
             */
            .flatMap { closePaymentOutcomeEvent ->
              closePaymentOutcomeEvent.fold(
                { Mono.empty() },
                { transactionClosedEvent ->
                  refundTransactionPipeline(
                    tx, transactionClosedEvent.data.responseOutcome, tracingInfo)
                })
            }
            .then()
            .onErrorResume { exception ->
              logger.error(
                "Got exception while retrying closePaymentV2 for transaction with id ${tx.transactionId}!",
                exception)

              val (statusCode, errorDescription) =
                if (exception is ClosePaymentErrorResponseException) {
                  Pair(exception.statusCode, exception.errorResponse?.description)
                } else {
                  Pair(null, null)
                }
              // transaction can be refund only for HTTP status code 422 and error response
              // description equals to "Node did not receive RPT yet" OR HTTP status code 400
              // and error response description equal to "Unacceptable outcome when token has
              // expired"
              val refundTransaction =
                (statusCode == HttpStatus.UNPROCESSABLE_ENTITY &&
                  errorDescription == NodeClient.NODE_DID_NOT_RECEIVE_RPT_YET_ERROR) ||
                  (statusCode == HttpStatus.BAD_REQUEST &&
                    errorDescription == NodeClient.UNACCEPTABLE_OUTCOME_TOKEN_EXPIRED)
              // retry event enqueued only for 5xx error responses or for other exceptions that
              // might happen during communication such as read timeout
              val enqueueRetryEvent =
                !refundTransaction && (statusCode == null || statusCode.is5xxServerError)
              logger.info(
                "Handling Nodo close payment error response. Status code: [{}], error description: [{}] -> refund transaction: [{}], enqueue retry event: [{}]",
                statusCode,
                errorDescription,
                refundTransaction,
                enqueueRetryEvent)
              if (refundTransaction) {
                // if transaction has to be refund perform refund
                refundTransactionPipeline(tx, TransactionClosureData.Outcome.KO, tracingInfo).then()
              } else {
                // otherwise check if another attempt has to be performed for close payment
                if (enqueueRetryEvent) {
                  closureRetryService.enqueueRetryEvent(tx, retryCount, tracingInfo).doOnError(
                    NoRetryAttemptsLeftException::class.java) { retryException ->
                    logger.error(
                      "No more attempts left for closure retry, refunding transaction",
                      retryException)
                  }
                } else {
                  // or skip more attempts for unhandled 4xx cases for which no more attempts have
                  // to be done
                  Mono.empty()
                }
              }
            }
        }
    val e = event.fold({ it }, { it })
    return if (tracingInfo != null) {
      runTracedPipelineWithDeadLetterQueue(
        checkPointer,
        closurePipeline,
        QueueEvent(e, tracingInfo),
        deadLetterTracedQueueAsyncClient,
        tracingUtils,
        this::class.simpleName!!)
    } else {
      runPipelineWithDeadLetterQueue(
        checkPointer,
        closurePipeline,
        BinaryData.fromObject(e).toBytes(),
        deadLetterTracedQueueAsyncClient)
    }
  }

  private fun refundTransactionPipeline(
    transaction: TransactionWithClosureError,
    closureOutcome: TransactionClosureData.Outcome,
    tracingInfo: TracingInfo?
  ): Mono<BaseTransaction> {
    val transactionAtPreviousState = transaction.transactionAtPreviousState()
    val wasAuthorized = wasTransactionAuthorized(transactionAtPreviousState)
    val toBeRefunded = wasAuthorized && closureOutcome == TransactionClosureData.Outcome.KO
    logger.info(
      "Transaction Nodo ClosePaymentV2 response outcome: ${closureOutcome}, was authorized: $wasAuthorized --> to be refunded: $toBeRefunded")
    val transactionWithCompletedAuthorization =
      getBaseTransactionWithCompletedAuthorization(transactionAtPreviousState)

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
          refundRetryService,
          tracingInfo)
      }
  }

  private fun wasTransactionCanceledByUser(
    transactionAtPreviousState:
      Optional<
        Either<BaseTransactionWithCancellationRequested, BaseTransactionWithCompletedAuthorization>>
  ): Boolean = transactionAtPreviousState.map { it.isLeft }.orElse(false)

  private fun wasTransactionAuthorized(
    transactionAtPreviousState:
      Optional<
        Either<BaseTransactionWithCancellationRequested, BaseTransactionWithCompletedAuthorization>>
  ): Boolean =
    transactionAtPreviousState
      .map {
        it.fold(
          { false },
          { tx ->
            tx.transactionAuthorizationCompletedData.authorizationResultDto ==
              AuthorizationResultDto.OK
          })
      }
      .orElseGet { false }

  private fun getBaseTransactionWithCompletedAuthorization(
    transactionAtPreviousState:
      Optional<
        Either<BaseTransactionWithCancellationRequested, BaseTransactionWithCompletedAuthorization>>
  ): Optional<BaseTransactionWithCompletedAuthorization> =
    transactionAtPreviousState.flatMap { either ->
      either.fold({ Optional.empty() }, { Optional.of(it) })
    }

  private fun updateTransactionStatus(
    transaction: BaseTransactionWithClosureError,
    closureOutcome: ClosePaymentOutcome,
    closePaymentResponseDto: ClosePaymentResponseDto,
    canceledByUser: Boolean,
    wasAuthorized: Boolean
  ): Mono<Either<TransactionClosureFailedEvent, TransactionClosedEvent>> {
    val outcome =
      when (closePaymentResponseDto.outcome) {
        ClosePaymentResponseDto.OutcomeEnum.OK -> TransactionClosureData.Outcome.OK
        ClosePaymentResponseDto.OutcomeEnum.KO -> TransactionClosureData.Outcome.KO
      }

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
        when (closureOutcome) {
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
}
