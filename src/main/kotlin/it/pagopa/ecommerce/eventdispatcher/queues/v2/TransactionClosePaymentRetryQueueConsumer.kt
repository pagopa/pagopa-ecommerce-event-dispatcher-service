package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.PgsTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithCompletedAuthorization
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NodeService
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

@Service("TransactionClosePaymentRetryQueueConsumerV2")
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
  @Autowired private val refundService: RefundService,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
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
      Either<QueueEvent<TransactionClosureErrorEvent>, QueueEvent<TransactionClosureRetriedEvent>>,
    checkPointer: Checkpointer
  ) = messageReceiver(parsedEvent, checkPointer, EmptyTransaction())

  fun messageReceiver(
    parsedEvent:
      Either<QueueEvent<TransactionClosureErrorEvent>, QueueEvent<TransactionClosureRetriedEvent>>,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Unit> {
    val event = parsedEvent.bimap({ it.event }, { it.event })
    val tracingInfo = parsedEvent.fold({ it.tracingInfo }, { it.tracingInfo })
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
                    ClosePaymentRequestV2Dto.OutcomeEnum.KO
                  },
                  {
                    /*
                     * retrying a close payment for an authorized transaction.
                     * Will be performed a close payment OK/KO based on the authorization outcome
                     */
                    trxWithAuthorizationCompleted ->
                    val transactionAuthGatewayData =
                      trxWithAuthorizationCompleted.transactionAuthorizationCompletedData
                        .transactionGatewayAuthorizationData
                    when (transactionAuthGatewayData) {
                      is PgsTransactionGatewayAuthorizationData ->
                        when (transactionAuthGatewayData.authorizationResultDto) {
                          AuthorizationResultDto.OK -> ClosePaymentRequestV2Dto.OutcomeEnum.OK
                          else -> ClosePaymentRequestV2Dto.OutcomeEnum.KO
                        }
                      is NpgTransactionGatewayAuthorizationData ->
                        when (transactionAuthGatewayData.operationResult) {
                          OperationResultDto.EXECUTED -> ClosePaymentRequestV2Dto.OutcomeEnum.OK
                          else -> ClosePaymentRequestV2Dto.OutcomeEnum.KO
                        }
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
              baseTransaction.flatMap { baseTransaction ->
                logger.error(
                  "Got exception while retrying closePaymentV2 for transaction with id ${baseTransaction.transactionId}!",
                  exception)
                /*
                 * The refund process is started only iff the previous transaction was authorized
                 * and the Nodo returned closePaymentV2 response outcome KO
                 */
                closureRetryService
                  .enqueueRetryEvent(baseTransaction, retryCount, tracingInfo)
                  .onErrorResume(NoRetryAttemptsLeftException::class.java) { exception ->
                    logger.error(
                      "No more attempts left for closure retry, refunding transaction", exception)
                    refundTransactionPipeline(tx, null, tracingInfo).then()
                  }
                  .then()
              }
            }
        }
    val e = event.fold({ it }, { it })
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      closurePipeline,
      QueueEvent(e, tracingInfo),
      deadLetterQueueAsyncClient,
      deadLetterTTLSeconds,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  private fun refundTransactionPipeline(
    transaction: TransactionWithClosureError,
    closureOutcome: TransactionClosureData.Outcome?,
    tracingInfo: TracingInfo?
  ): Mono<BaseTransaction> {
    val transactionAtPreviousState = transaction.transactionAtPreviousState()
    val wasAuthorized = wasTransactionAuthorized(transactionAtPreviousState)
    val nodoOutcome = closureOutcome ?: TransactionClosureData.Outcome.KO
    val toBeRefunded = wasAuthorized && nodoOutcome == TransactionClosureData.Outcome.KO
    logger.info(
      "Transaction Nodo ClosePaymentV2 response outcome: ${nodoOutcome}, was authorized: $wasAuthorized --> to be refunded: $toBeRefunded")
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
          refundService,
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
            val transactionGatewayData =
              tx.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData
            when (transactionGatewayData) {
              is PgsTransactionGatewayAuthorizationData ->
                transactionGatewayData.authorizationResultDto == AuthorizationResultDto.OK
              is NpgTransactionGatewayAuthorizationData ->
                transactionGatewayData.operationResult == OperationResultDto.EXECUTED
            }
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
    closureOutcome: ClosePaymentRequestV2Dto.OutcomeEnum,
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
          ClosePaymentRequestV2Dto.OutcomeEnum.OK -> TransactionStatusDto.CLOSED
          ClosePaymentRequestV2Dto.OutcomeEnum.KO -> TransactionStatusDto.UNAUTHORIZED
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
