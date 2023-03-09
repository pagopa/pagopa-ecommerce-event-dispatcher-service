package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.TransactionExpiredEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundedData
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRefundRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.eventretry.RefundRetryService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

/**
 * Event consumer for transactions to refund. These events are input in the event queue only when a
 * transaction is stuck in an REFUND_REQUESTED state **and** needs to be reverted
 */
@Service
class TransactionsRefundConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val refundRetryService: RefundRetryService
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsRefundConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
    val idFromActivatedEvent =
      data.toObjectAsync(TransactionExpiredEvent::class.java).map { it.transactionId }
    val idFromRefundedEvent =
      data.toObjectAsync(TransactionRefundRetriedEvent::class.java).map { it.transactionId }

    return Mono.firstWithValue(idFromActivatedEvent, idFromRefundedEvent)
  }

  @ServiceActivator(inputChannel = "transactionsrefundchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer
  ): Mono<Void> {
    val checkpoint = checkpointer.success()

    val transactionId = getTransactionIdFromPayload(BinaryData.fromBytes(payload))

    val refundPipeline =
      transactionId
        .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it) }
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
        .filter { it.status == TransactionStatusDto.REFUND_REQUESTED }
        .switchIfEmpty {
          return@switchIfEmpty transactionId
            .doOnNext {
              logger.info("Transaction $it was not previously authorized. No refund needed")
            }
            .flatMap { Mono.empty() }
        }
        .doOnNext {
          logger.info("Handling refund request for transaction with id ${it.transactionId.value}")
        }
        .cast(BaseTransactionWithRefundRequested::class.java)
        .flatMap { tx ->
          refundTransaction(
            tx,
            transactionsRefundedEventStoreRepository,
            transactionsViewRepository,
            paymentGatewayClient,
            refundRetryService)
        }
        .onErrorMap {
          logger.error(
            "Transaction requestRefund error for transaction ${transactionId.block()} : ${it.message}")
          // TODO retry
          it
        }

    return checkpoint.then(refundPipeline).then()
  }
}
