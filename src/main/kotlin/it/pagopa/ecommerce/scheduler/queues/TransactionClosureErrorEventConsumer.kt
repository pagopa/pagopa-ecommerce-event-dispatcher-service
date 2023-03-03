package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCompletedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.scheduler.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
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
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosureErrorEventConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
    val idFromClosureErrorEvent =
      data.toObjectAsync(TransactionClosureErrorEvent::class.java).map { it.transactionId }
    val idFromClosureRetriedEvent =
      data.toObjectAsync(TransactionClosureRetriedEvent::class.java).map { it.transactionId }

    return Mono.firstWithValue(idFromClosureErrorEvent, idFromClosureRetriedEvent)
  }

  @ServiceActivator(inputChannel = "transactionclosureschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer
  ): Mono<Either<TransactionClosureFailedEvent, TransactionClosedEvent>> {
    val checkpoint = checkpointer.success()

    val transactionId = getTransactionIdFromPayload(BinaryData.fromBytes(payload))

    val closurePipeline =
      transactionId
        .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it) }
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
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
        .cast(BaseTransactionWithClosureError::class.java)
        .flatMap { tx ->
          val closureOutcome =
            when (val transactionAtPreviousState = tx.transactionAtPreviousState) {
              is BaseTransactionWithCompletedAuthorization -> {
                when (transactionAtPreviousState.transactionAuthorizationCompletedData
                  .authorizationResultDto) {
                  AuthorizationResultDto.OK -> ClosePaymentRequestV2Dto.OutcomeEnum.OK
                  AuthorizationResultDto.KO -> ClosePaymentRequestV2Dto.OutcomeEnum.KO
                  null ->
                    return@flatMap Mono.error(
                      RuntimeException("authorizationResult in status update event is null!"))
                }
              }
              is BaseTransactionWithCancellationRequested -> ClosePaymentRequestV2Dto.OutcomeEnum.KO
              else -> {
                return@flatMap Mono.error(
                  RuntimeException(
                    "Unexpected transactionAtPreviousStep: ${tx.transactionAtPreviousState}"))
              }
            }

          mono { nodeService.closePayment(tx.transactionId.value, closureOutcome) }
            .flatMap { closePaymentResponse ->
              updateTransactionStatus(tx, closureOutcome, closePaymentResponse)
            }
        }
        .onErrorMap { exception ->
          // TODO: Add appropriate retrying logic + enqueueing of retry event

          logger.error("Got exception while retrying closePayment!", exception)
          return@onErrorMap exception
        }

    return checkpoint.then(closurePipeline)
  }

  private fun updateTransactionStatus(
    transaction: BaseTransactionWithClosureError,
    closureOutcome: ClosePaymentRequestV2Dto.OutcomeEnum,
    closePaymentResponseDto: ClosePaymentResponseDto
  ): Mono<Either<TransactionClosureFailedEvent, TransactionClosedEvent>> {
    val outcome =
      when (closePaymentResponseDto.outcome) {
        ClosePaymentResponseDto.OutcomeEnum.OK -> TransactionClosureData.Outcome.OK
        ClosePaymentResponseDto.OutcomeEnum.KO -> TransactionClosureData.Outcome.KO
      }

    val event: Either<TransactionClosureFailedEvent, TransactionClosedEvent> =
      when (outcome) {
        TransactionClosureData.Outcome.OK ->
          Either.right(
            TransactionClosedEvent(
              transaction.transactionId.value.toString(), TransactionClosureData(outcome)))
        TransactionClosureData.Outcome.KO ->
          Either.left(
            TransactionClosureFailedEvent(
              transaction.transactionId.value.toString(), TransactionClosureData(outcome)))
      }

    val newStatus =
      when (closureOutcome) {
        ClosePaymentRequestV2Dto.OutcomeEnum.OK -> TransactionStatusDto.CLOSED
        ClosePaymentRequestV2Dto.OutcomeEnum.KO -> TransactionStatusDto.UNAUTHORIZED
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
