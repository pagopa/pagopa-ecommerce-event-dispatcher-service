package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.Transaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

@Service("TransactionAuthorizationRequestedRetryQueueConsumerV2")
class TransactionAuthorizationRequestedRetryQueueConsumer(
  @Autowired private val transactionsServiceClient: TransactionsServiceClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService,
  @Autowired private val authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {
  var logger: Logger =
    LoggerFactory.getLogger(TransactionAuthorizationRequestedRetryQueueConsumer::class.java)

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionAuthorizationRequestedRetriedEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val event = parsedEvent.event
    val tracingInfo = parsedEvent.tracingInfo
    val transactionId = event.transactionId
    val authorizationRequestedRetryPipeline =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(transactionId)
        .reduce(EmptyTransaction(), Transaction::applyEvent)
        .cast(BaseTransaction::class.java)
        .doOnNext {
          logger.info(
            "Performing attempt number ${event.data.retryCount} for NPG getState invocation")
        }
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
          handleGetState(
            tx,
            authorizationStateRetrieverRetryService,
            authorizationStateRetrieverService,
            transactionsServiceClient,
            tracingInfo,
            event.data.retryCount)
        }
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      authorizationRequestedRetryPipeline,
      QueueEvent(event, tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }
}
