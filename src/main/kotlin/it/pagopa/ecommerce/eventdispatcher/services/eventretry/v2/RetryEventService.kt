package it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.JsonSerializerProvider
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRetriedData
import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransaction
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
import java.time.ZonedDateTime

abstract class RetryEventService<E>(
  private val queueAsyncClient: QueueAsyncClient,
  private val retryOffset: Int,
  private val maxAttempts: Int,
  private val viewRepository: TransactionsViewRepository,
  private val retryEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRetriedData>,
  protected val logger: Logger = LoggerFactory.getLogger(RetryEventService::class.java),
  private val transientQueuesTTLSeconds: Int,
  private val strictSerializerProviderV2: JsonSerializerProvider
) where E : TransactionEvent<BaseTransactionRetriedData> {

  fun enqueueRetryEvent(
    baseTransaction: BaseTransaction,
    retriedCount: Int,
    tracingInfo: TracingInfo?,
    transactionGatewayAuthorizationData: TransactionGatewayAuthorizationData? = null,
    throwable: Throwable? = null
  ): Mono<Void> {
    val retryEvent =
      buildRetryEvent(
        baseTransaction.transactionId,
        TransactionRetriedData(retriedCount + 1),
        transactionGatewayAuthorizationData,
        throwable)
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
    transactionRetriedData: TransactionRetriedData,
    transactionGatewayAuthorizationData: TransactionGatewayAuthorizationData?,
    throwable: Throwable?
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
      .cast(Transaction::class.java)
      .flatMap {
        it.status = newStatus
        it.lastProcessedEventAt = ZonedDateTime.parse(event.creationDate).toInstant().toEpochMilli()
        viewRepository.save(it).flatMap { Mono.just(event) }
      }

  private fun enqueueMessage(
    event: E,
    visibilityTimeout: Duration,
    tracingInfo: TracingInfo?
  ): Mono<Void> =
    queueAsyncClient
      .sendMessageWithResponse(
        queuePayload(event, tracingInfo),
        visibilityTimeout,
        Duration.ofSeconds(transientQueuesTTLSeconds.toLong()), // timeToLive
      )
      .doOnNext {
        logger.info(
          "Event: [$event] successfully sent with visibility timeout: [${it.value.timeNextVisible}] ms to queue: [${queueAsyncClient.queueName}]")
      }
      .then()
      .doOnError { exception -> logger.error("Error sending event: [${event}].", exception) }

  private fun queuePayload(event: E, tracingInfo: TracingInfo?): BinaryData {
    return if (tracingInfo != null) {
      BinaryData.fromObject(
        QueueEvent(event, tracingInfo), strictSerializerProviderV2.createInstance())
    } else {
      BinaryData.fromObject(event, strictSerializerProviderV2.createInstance())
    }
  }
}
