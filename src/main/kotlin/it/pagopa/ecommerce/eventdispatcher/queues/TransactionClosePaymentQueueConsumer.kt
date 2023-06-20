package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.NodeService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.ClosureRetryService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
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
  @Value("\${azurestorage.queues.deadLetterQueue.ttlMinutes}") private val deadLetterTTLMinutes: Int
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
    return data.toObjectAsync(TransactionUserCanceledEvent::class.java).map { it.transactionId }
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
    val binaryData = BinaryData.fromBytes(payload)
    val transactionId = getTransactionIdFromPayload(binaryData)
    val baseTransaction =
      reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
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
                logger.error(
                  "Got exception while calling closePaymentV2 for transaction with id ${baseTransaction.transactionId}!",
                  exception)

                mono { baseTransaction }
                  .map { tx -> TransactionClosureErrorEvent(tx.transactionId.value().toString()) }
                  .flatMap { transactionClosureErrorEvent ->
                    transactionClosureErrorEventStoreRepository.save(transactionClosureErrorEvent)
                  }
                  .flatMap {
                    transactionsViewRepository.findByTransactionId(
                      baseTransaction.transactionId.value())
                  }
                  .flatMap { tx ->
                    tx.status = TransactionStatusDto.CLOSURE_ERROR
                    transactionsViewRepository.save(tx)
                  }
                  .flatMap {
                    reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
                  }
                  .flatMap { transactionUpdated ->
                    closureRetryService.enqueueRetryEvent(transactionUpdated, 0).doOnError(
                      NoRetryAttemptsLeftException::class.java) { exception ->
                      logger.error("No more attempts left for closure retry", exception)
                    }
                  }
              }
            }
        }
        .then()

    return runPipelineWithDeadLetterQueue(
      checkPointer, closurePipeline, payload, deadLetterQueueAsyncClient, deadLetterTTLMinutes)
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
      transactionsViewRepository.findByTransactionId(transaction.transactionId.value())

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
