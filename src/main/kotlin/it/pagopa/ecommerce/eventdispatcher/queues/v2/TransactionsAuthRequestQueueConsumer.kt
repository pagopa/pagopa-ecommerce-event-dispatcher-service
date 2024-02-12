package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundedData
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.Transaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgStateService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

/**
 * Event consumer for transactions to refund. These events are input in the event queue only when a
 * transaction is stuck in an REFUND_REQUESTED state **and** needs to be reverted
 */
@Service("TransactionsAuthRequestedQueueConsumer")
class TransactionsAuthRequestQueueConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsServiceClient: TransactionsServiceClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val npgStateService: NpgStateService,
  @Autowired private val npgStateRetryService: NpgStateService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsAuthRequestQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(event: TransactionAuthorizationRequestedEvent): String {
    return event.transactionId
  }

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionAuthorizationRequestedEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val event = parsedEvent.event
    val tracingInfo = parsedEvent.tracingInfo
    val transactionId = getTransactionIdFromPayload(event)
    val authorizationRequestedPipeline =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
        .filter { it.status == TransactionStatusDto.AUTHORIZATION_REQUESTED }
        .switchIfEmpty {
          logger.info(
            "Transaction $transactionId is not is Authorization Requested status. No more action needed")
          Mono.empty()
        }
        .doOnNext {
          logger.info(
            "Handling get state request for transaction with id ${it.transactionId.value()}")
        }
        .cast(BaseTransactionWithRequestedAuthorization::class.java)
        .flatMap { tx ->
          handleGetState(tx, event, npgStateService, transactionsServiceClient, 3, tracingInfo)
        }
    val e = QueueEvent(event, tracingInfo)
    return runTracedPipelineWithDeadLetterQueue( // CHECK THIS METHOD
      checkPointer,
      authorizationRequestedPipeline,
      e,
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }
}
