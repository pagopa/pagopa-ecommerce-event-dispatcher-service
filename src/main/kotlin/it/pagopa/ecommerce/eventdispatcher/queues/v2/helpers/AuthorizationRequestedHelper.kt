package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.Transaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.queues.v2.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

data class AuthorizationRequestedEvent(
  val requested: QueueEvent<TransactionAuthorizationRequestedEvent>?,
  val retried: QueueEvent<TransactionAuthorizationRequestedRetriedEvent>?
) {
  init {
    require(listOfNotNull(requested, retried).size == 1) { "Only one event must be non-null!" }
  }

  companion object {
    fun requested(
      event: QueueEvent<TransactionAuthorizationRequestedEvent>
    ): AuthorizationRequestedEvent = AuthorizationRequestedEvent(event, null)

    fun retried(
      event: QueueEvent<TransactionAuthorizationRequestedRetriedEvent>
    ): AuthorizationRequestedEvent = AuthorizationRequestedEvent(null, event)
  }

  fun <T> fold(
    onRequested: (QueueEvent<TransactionAuthorizationRequestedEvent>) -> T,
    onRetried: (QueueEvent<TransactionAuthorizationRequestedRetriedEvent>) -> T,
  ): T {
    return checkNotNull(
      when {
        requested != null -> onRequested(requested)
        retried != null -> onRetried(retried)
        else -> null
      }) { "No variant of `AuthorizationRequestedEvent` is non-null!" }
  }
}

/**
 * This helper implements the business logic related to handling calling `getState` from NPG. In
 * particular, the [getAuthorizationState] method does the following:
 * - checks for the transaction current status
 * - determines whether the transaction was requesting authorization via NPG
 * - calls NPG's `getSTate`
 * - calls transactions-service PATCH auth-request
 * - enqueues a retry event in case of error
 */
@Component
class AuthorizationRequestedHelper(
  @Autowired private val transactionsServiceClient: TransactionsServiceClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService,
  @Autowired private val authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  var logger: Logger = LoggerFactory.getLogger(AuthorizationRequestedHelper::class.java)

  fun authorizationStateRetrieve(
    parsedEvent: AuthorizationRequestedEvent,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val tracingInfo = getTracingInfo(parsedEvent)
    val transactionId = getTransactionId(parsedEvent)
    val retryCount = getRetryCount(parsedEvent)

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
          handleGetState(
            tx = tx,
            authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
            authorizationStateRetrieverService = authorizationStateRetrieverService,
            transactionsServiceClient = transactionsServiceClient,
            tracingInfo = tracingInfo,
            retryCount = retryCount)
        }
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      authorizationRequestedPipeline,
      QueueEvent(parsedEvent.fold({ it }, { it }).event, tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  private fun getTracingInfo(event: AuthorizationRequestedEvent): TracingInfo {
    return event.fold({ it.tracingInfo }, { it.tracingInfo })
  }

  private fun getTransactionId(event: AuthorizationRequestedEvent): String {
    return event.fold({ it.event.transactionId }, { it.event.transactionId })
  }

  private fun getRetryCount(event: AuthorizationRequestedEvent): Int {
    return event.fold({ 0 }, { it.event.data.retryCount })
  }
}
