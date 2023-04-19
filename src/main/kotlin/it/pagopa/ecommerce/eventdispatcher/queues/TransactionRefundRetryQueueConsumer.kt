package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.Transaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to refund retry. This consumer's responsibilities are to handle
 * refund process retry for a given transaction
 */
@Service
class TransactionRefundRetryQueueConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val refundRetryService: RefundRetryService
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionRefundRetryQueueConsumer::class.java)

  private fun parseInputEvent(data: BinaryData): Mono<TransactionRefundRetriedEvent> {
    return data.toObjectAsync(TransactionRefundRetriedEvent::class.java)
  }

  @ServiceActivator(inputChannel = "transactionrefundretrychannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkpointer: Checkpointer
  ): Mono<Void> {
    val checkpoint = checkpointer.success()
    val event = parseInputEvent(BinaryData.fromBytes(payload))
    val baseTransaction =
      event
        .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it.transactionId) }
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
    val refundPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.REFUND_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = TransactionStatusDto.REFUND_ERROR,
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .flatMap { tx ->
          event.flatMap { event ->
            refundTransaction(
              tx,
              transactionsRefundedEventStoreRepository,
              transactionsViewRepository,
              paymentGatewayClient,
              refundRetryService,
              event.data.retryCount)
          }
        }
    return checkpoint.then(refundPipeline).then()
  }
}
