package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
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
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.scheduler.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.ecommerce.scheduler.services.eventretry.ClosureRetryService
import it.pagopa.ecommerce.scheduler.services.eventretry.RefundRetryService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class TransactionClosureErrorEventConsumer(
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
  @Autowired private val refundRetryService: RefundRetryService
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosureErrorEventConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
    val idFromClosureErrorEvent =
      data.toObjectAsync(TransactionClosureErrorEvent::class.java).map { it.transactionId }
    val idFromClosureRetriedEvent =
      data.toObjectAsync(TransactionClosureRetriedEvent::class.java).map { it.transactionId }

    return Mono.firstWithValue(idFromClosureErrorEvent, idFromClosureRetriedEvent)
  }

  private fun getRetryCountFromPayload(data: BinaryData): Mono<Int> {
    return data
      .toObjectAsync(TransactionClosureRetriedEvent::class.java)
      .map { Optional.ofNullable(it.data.retryCount).orElse(0) }
      .onErrorResume { Mono.just(0) }
  }

  @ServiceActivator(inputChannel = "transactionclosureschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ) = messageReceiver(payload, checkPointer, EmptyTransaction())

  fun messageReceiver(
    payload: ByteArray,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Void> {
    val checkpoint = checkPointer.success()
    val binaryData = BinaryData.fromBytes(payload)
    val transactionId = getTransactionIdFromPayload(binaryData)
    val retryCount = getRetryCountFromPayload(binaryData)
    val baseTransaction =
      reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
    val closurePipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value}: ${it.status}")

          if (it.status != TransactionStatusDto.CLOSURE_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = TransactionStatusDto.CLOSURE_ERROR,
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
                    when (trxWithAuthorizationCompleted.transactionAuthorizationCompletedData
                      .authorizationResultDto) {
                      AuthorizationResultDto.OK -> ClosePaymentRequestV2Dto.OutcomeEnum.OK
                      AuthorizationResultDto.KO -> ClosePaymentRequestV2Dto.OutcomeEnum.KO
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
          mono { nodeService.closePayment(tx.transactionId.value, closureOutcome) }
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
            .flatMap {
              it.fold(
                { Mono.empty() },
                { transactionClosedEvent ->
                  refundTransactionPipeline(tx, transactionClosedEvent.data.responseOutcome)
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
                retryCount
                  .flatMap { retryCount ->
                    closureRetryService.enqueueRetryEvent(baseTransaction, retryCount)
                  }
                  .onErrorResume(NoRetryAttemptsLeftException::class.java) { exception ->
                    logger.error(
                      "No more attempts left for closure retry, refunding transaction", exception)
                    refundTransactionPipeline(tx, null).then()
                  }
                  .then()
              }
            }
        }

    return checkpoint.then(closurePipeline).then()
  }

  private fun refundTransactionPipeline(
    transaction: TransactionWithClosureError,
    closureOutcome: TransactionClosureData.Outcome?
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
          refundRetryService)
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
            transaction.transactionId.value.toString(), TransactionClosureData(outcome)))
      } else {

        Either.right(
          TransactionClosedEvent(
            transaction.transactionId.value.toString(), TransactionClosureData(outcome)))
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
    logger.info("Updating transaction {} status to {}", transaction.transactionId.value, newStatus)

    val transactionUpdate =
      transactionsViewRepository.findByTransactionId(transaction.transactionId.value.toString())

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
