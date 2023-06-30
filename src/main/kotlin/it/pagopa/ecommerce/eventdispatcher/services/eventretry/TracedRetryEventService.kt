package it.pagopa.ecommerce.eventdispatcher.services.eventretry

import it.pagopa.ecommerce.commons.client.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRetriedData
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TooLateRetryAttemptException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import java.time.Duration
import java.time.Instant
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

abstract class TracedRetryEventService<E>(
  private val queueAsyncClient: QueueAsyncClient,
  private val retryOffset: Int,
  private val maxAttempts: Int,
  private val viewRepository: TransactionsViewRepository,
  private val retryEventStoreRepository: TransactionsEventStoreRepository<TransactionRetriedData>,
  protected val logger: Logger = LoggerFactory.getLogger(TracedRetryEventService::class.java),
  private val transientQueuesTTLSeconds: Int
) where E : TransactionEvent<TransactionRetriedData> {

  fun enqueueRetryEvent(
    baseTransaction: BaseTransaction,
    retriedCount: Int,
    tracingInfo: TracingInfo
  ): Mono<Void> {
    val retryEvent =
      buildRetryEvent(baseTransaction.transactionId, TransactionRetriedData(retriedCount + 1))
    val visibilityTimeout = Duration.ofSeconds((retryOffset * retryEvent.data.retryCount).toLong())
    return Mono.just(retryEvent)
      .filter { it.data.retryCount <= maxAttempts }
      .switchIfEmpty(
        Mono.error(
          NoRetryAttemptsLeftException(
            eventCode = retryEvent.eventCode, transactionId = baseTransaction.transactionId)))
      .filter { validateRetryEventVisibilityTimeout(baseTransaction, visibilityTimeout) }
      .switchIfEmpty {
        Mono.error(
          TooLateRetryAttemptException(
            eventCode = retryEvent.eventCode,
            transactionId = baseTransaction.transactionId,
            visibilityTimeout = Instant.now().plus(visibilityTimeout)))
      }
      .flatMap { storeEventAndUpdateView(it, newTransactionStatus()) }
      .flatMap { enqueueMessage(it, visibilityTimeout, tracingInfo) }
      .doOnError {
        logger.error(
          "Error processing retry event for transaction with id: [${retryEvent.transactionId}]", it)
      }
  }

  abstract fun buildRetryEvent(
    transactionId: TransactionId,
    transactionRetriedData: TransactionRetriedData
  ): E

  abstract fun newTransactionStatus(): TransactionStatusDto

  abstract fun validateRetryEventVisibilityTimeout(
    baseTransaction: BaseTransaction,
    visibilityTimeout: Duration
  ): Boolean

  private fun storeEventAndUpdateView(event: E, newStatus: TransactionStatusDto): Mono<E> =
    Mono.just(event)
      .flatMap { retryEventStoreRepository.save(it) }
      .flatMap { viewRepository.findByTransactionId(it.transactionId) }
      .flatMap {
        it.status = newStatus
        viewRepository.save(it).flatMap { Mono.just(event) }
      }

  private fun enqueueMessage(
    event: E,
    visibilityTimeout: Duration,
    tracingInfo: TracingInfo
  ): Mono<Void> =
    Mono.just(event).flatMap { eventToSend ->
      queueAsyncClient
        .sendMessageWithResponse(
          QueueEvent(event, tracingInfo),
          visibilityTimeout,
          Duration.ofSeconds(transientQueuesTTLSeconds.toLong()), // timeToLive
        )
        .doOnNext {
          logger.info(
            "Event: [$event] successfully sent with visibility timeout: [${it.value.timeNextVisible}] ms to queue: [${queueAsyncClient.queueName}]")
        }
        .then()
        .doOnError { exception -> logger.error("Error sending event: [${event}].", exception) }
    }
}
