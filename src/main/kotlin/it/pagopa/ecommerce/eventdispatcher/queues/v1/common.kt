package it.pagopa.ecommerce.eventdispatcher.queues.v1

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.queues.v1.QueueCommonsLogger.logger
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v1.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.ZonedDateTime
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

object QueueCommonsLogger {
  val logger: Logger = LoggerFactory.getLogger(QueueCommonsLogger::class.java)
}

fun updateTransactionToExpired(
  transaction: BaseTransaction,
  transactionsExpiredEventStoreRepository: TransactionsEventStoreRepository<TransactionExpiredData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {

  return transactionsExpiredEventStoreRepository
    .save(
      TransactionExpiredEvent(
        transaction.transactionId.value(), TransactionExpiredData(transaction.status)))
    .then(
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)
        .flatMap { tx ->
          tx.status = getExpiredTransactionStatus(transaction)
          transactionsViewRepository.save(tx)
        })
    .doOnSuccess {
      logger.info("Transaction expired for transaction ${transaction.transactionId.value()}")
    }
    .doOnError {
      logger.error(
        "Transaction expired error for transaction ${transaction.transactionId.value()} : ${it.message}")
    }
    .thenReturn(transaction)
}

fun getExpiredTransactionStatus(transaction: BaseTransaction): TransactionStatusDto =
  when (transaction) {
    is BaseTransactionWithRequestedAuthorization -> TransactionStatusDto.EXPIRED
    is BaseTransactionWithCancellationRequested -> TransactionStatusDto.CANCELLATION_EXPIRED
    is TransactionWithClosureError ->
      transaction
        .transactionAtPreviousState()
        .map {
          it.fold({ TransactionStatusDto.CANCELLATION_EXPIRED }, { TransactionStatusDto.EXPIRED })
        }
        .orElse(TransactionStatusDto.EXPIRED)
    else -> TransactionStatusDto.EXPIRED_NOT_AUTHORIZED
  }

fun updateTransactionToRefundRequested(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  return updateTransactionWithRefundEvent(
    transaction,
    transactionsRefundedEventStoreRepository,
    transactionsViewRepository,
    TransactionRefundRequestedEvent(
      transaction.transactionId.value(), TransactionRefundedData(transaction.status)),
    TransactionStatusDto.REFUND_REQUESTED)
}

fun updateTransactionToRefundError(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  return updateTransactionWithRefundEvent(
    transaction,
    transactionsRefundedEventStoreRepository,
    transactionsViewRepository,
    TransactionRefundErrorEvent(
      transaction.transactionId.value(), TransactionRefundedData(transaction.status)),
    TransactionStatusDto.REFUND_ERROR)
}

fun updateTransactionToRefunded(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
): Mono<BaseTransaction> {
  return updateTransactionWithRefundEvent(
    transaction,
    transactionsRefundedEventStoreRepository,
    transactionsViewRepository,
    TransactionRefundedEvent(
      transaction.transactionId.value(), TransactionRefundedData(transaction.status)),
    TransactionStatusDto.REFUNDED)
}

fun updateTransactionWithRefundEvent(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  event: TransactionEvent<TransactionRefundedData>,
  status: TransactionStatusDto
): Mono<BaseTransaction> {
  return transactionsRefundedEventStoreRepository
    .save(event)
    .then(
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)
        .flatMap { tx ->
          tx.status = status
          transactionsViewRepository.save(tx)
        })
    .doOnSuccess {
      logger.info(
        "Updated event for transaction with id ${transaction.transactionId.value()} to status $status")
    }
    .thenReturn(transaction)
}

fun refundTransaction(
  tx: BaseTransaction,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  paymentGatewayClient: PaymentGatewayClient,
  refundRetryService: RefundRetryService,
  tracingInfo: TracingInfo?,
  retryCount: Int = 0
): Mono<BaseTransaction> {
  return Mono.empty()
}

fun isTransactionRefundable(tx: BaseTransaction): Boolean {
  val wasAuthorizationRequested = wasAuthorizationRequested(tx)
  val wasSendPaymentResultOutcomeKO = wasSendPaymentResultOutcomeKO(tx)
  val wasAuthorizationDenied = wasAuthorizationDenied(tx)
  val wasClosePaymentResponseOutcomeKO = wasClosePaymentResponseOutcomeKo(tx)

  val isTransactionRefundable =
    when (tx) {
      // transaction for which a send payment result was received --> refund =
      // sendPaymentResultOutcome == KO
      is BaseTransactionWithRequestedUserReceipt -> wasSendPaymentResultOutcomeKO
      // transaction stuck after closePayment --> refund = closePaymentResponseOutcome == KO
      is BaseTransactionClosed -> wasClosePaymentResponseOutcomeKO
      // transaction stuck at closure error (no close payment vs Nodo) --> refund =
      // check previous transaction status
      is TransactionWithClosureError -> isTransactionRefundable(tx.transactionAtPreviousState)
      // transaction stuck at authorization completed status --> refund = PGS auth outcome != KO
      is BaseTransactionWithCompletedAuthorization -> !wasAuthorizationDenied
      // transaction in expired status (expiration event sent by batch) --> refund =
      // check previous transaction status
      is BaseTransactionExpired -> isTransactionRefundable(tx.transactionAtPreviousState)
      // transaction stuck at previous steps (authorization requested, activation...) --> refund =
      // authorization was requested to PGS
      else -> wasAuthorizationRequested
    }
  logger.info(
    "Transaction with id ${tx.transactionId.value()} : authorization requested: $wasAuthorizationRequested, authorization denied: $wasAuthorizationDenied, closePaymentResponse.outcome KO: $wasClosePaymentResponseOutcomeKO sendPaymentResult.outcome KO : $wasSendPaymentResultOutcomeKO --> is refundable: $isTransactionRefundable")
  return isTransactionRefundable
}

fun isRefundableCheckRequired(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionExpired -> isRefundableCheckRequired(tx.transactionAtPreviousState)
    is BaseTransactionWithClosureError -> isRefundableCheckRequired(tx.transactionAtPreviousState)
    else -> tx.status == TransactionStatusDto.AUTHORIZATION_COMPLETED && !wasAuthorizationDenied(tx)
  }

fun wasSendPaymentResultOutcomeKO(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionWithRequestedUserReceipt ->
      tx.transactionUserReceiptData.responseOutcome == TransactionUserReceiptData.Outcome.KO
    is BaseTransactionExpired -> wasSendPaymentResultOutcomeKO(tx.transactionAtPreviousState)
    else -> false
  }

fun wasAuthorizationRequested(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionWithRequestedAuthorization -> true
    is TransactionWithClosureError ->
      tx
        .transactionAtPreviousState()
        .map { txAtPreviousStep -> txAtPreviousStep.isRight }
        .orElse(false)
    else -> false
  }

fun getAuthorizationOutcome(tx: BaseTransaction): AuthorizationResultDto? =
  when (tx) {
    is BaseTransactionWithCompletedAuthorization ->
      tx.transactionAuthorizationCompletedData.authorizationResultDto
    is TransactionWithClosureError -> getAuthorizationOutcome(tx.transactionAtPreviousState)
    is BaseTransactionExpired -> getAuthorizationOutcome(tx.transactionAtPreviousState)
    else -> null
  }

fun wasAuthorizationDenied(tx: BaseTransaction): Boolean =
  getAuthorizationOutcome(tx) == AuthorizationResultDto.KO

fun isTransactionExpired(tx: BaseTransaction): Boolean =
  tx.status == TransactionStatusDto.EXPIRED ||
    tx.status == TransactionStatusDto.EXPIRED_NOT_AUTHORIZED ||
    tx.status == TransactionStatusDto.CANCELLATION_EXPIRED

fun reduceEvents(
  transactionId: Mono<String>,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) = reduceEvents(transactionId, transactionsEventStoreRepository, EmptyTransaction())

fun reduceEvents(
  transactionId: Mono<String>,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  emptyTransaction: EmptyTransaction
) =
  reduceEvents(
    transactionId.flatMapMany {
      transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(it).map {
        it as TransactionEvent<Any>
      }
    },
    emptyTransaction)

fun <T> reduceEvents(events: Flux<TransactionEvent<T>>) = reduceEvents(events, EmptyTransaction())

fun <T> reduceEvents(events: Flux<TransactionEvent<T>>, emptyTransaction: EmptyTransaction) =
  events
    .reduce(emptyTransaction, it.pagopa.ecommerce.commons.domain.v1.Transaction::applyEvent)
    .cast(BaseTransaction::class.java)

fun updateNotifiedTransactionStatus(
  transaction: BaseTransactionWithRequestedUserReceipt,
  transactionsViewRepository: TransactionsViewRepository,
  transactionUserReceiptRepository: TransactionsEventStoreRepository<TransactionUserReceiptData>
): Mono<TransactionUserReceiptAddedEvent> {
  val newStatus =
    when (transaction.transactionUserReceiptData.responseOutcome!!) {
      TransactionUserReceiptData.Outcome.OK -> TransactionStatusDto.NOTIFIED_OK
      TransactionUserReceiptData.Outcome.KO -> TransactionStatusDto.NOTIFIED_KO
      else ->
        throw RuntimeException(
          "Unexpected transaction user receipt data response outcome ${transaction.transactionUserReceiptData.responseOutcome} for transaction with id: ${transaction.transactionId.value()}")
    }
  val event =
    TransactionUserReceiptAddedEvent(
      transaction.transactionId.value(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value())
    .cast(Transaction::class.java)
    .flatMap { tx ->
      tx.status = newStatus
      transactionsViewRepository.save(tx)
    }
    .flatMap { transactionUserReceiptRepository.save(event) }
}

fun updateNotificationErrorTransactionStatus(
  transaction: BaseTransactionWithRequestedUserReceipt,
  transactionsViewRepository: TransactionsViewRepository,
  transactionUserReceiptRepository: TransactionsEventStoreRepository<TransactionUserReceiptData>
): Mono<TransactionUserReceiptAddErrorEvent> {
  val newStatus = TransactionStatusDto.NOTIFICATION_ERROR
  val event =
    TransactionUserReceiptAddErrorEvent(
      transaction.transactionId.value(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value())
    .cast(Transaction::class.java)
    .flatMap { tx ->
      tx.status = newStatus
      transactionsViewRepository.save(tx)
    }
    .flatMap { transactionUserReceiptRepository.save(event) }
}

fun notificationRefundTransactionPipeline(
  transaction: BaseTransactionWithRequestedUserReceipt,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  paymentGatewayClient: PaymentGatewayClient,
  refundRetryService: RefundRetryService,
  tracingInfo: TracingInfo?,
): Mono<BaseTransaction> {
  val userReceiptOutcome = transaction.transactionUserReceiptData.responseOutcome
  val toBeRefunded = userReceiptOutcome == TransactionUserReceiptData.Outcome.KO
  logger.info(
    "Transaction Nodo sendPaymentResult response outcome: $userReceiptOutcome --> to be refunded: $toBeRefunded")
  return Mono.just(transaction)
    .filter { toBeRefunded }
    .flatMap { tx ->
      updateTransactionToRefundRequested(
        tx, transactionsRefundedEventStoreRepository, transactionsViewRepository)
    }
    .flatMap {
      refundTransaction(
        transaction,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        paymentGatewayClient,
        refundRetryService,
        tracingInfo)
    }
}

fun <T> runPipelineWithDeadLetterQueue(
  checkPointer: Checkpointer,
  pipeline: Mono<T>,
  eventPayload: ByteArray,
  deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient
): Mono<Unit> {
  val binaryData = BinaryData.fromBytes(eventPayload)
  val eventLogString = "event payload: ${eventPayload.toString(StandardCharsets.UTF_8)}"
  return checkPointer
    .success()
    .doOnSuccess { logger.info("Checkpoint performed successfully for event $eventLogString") }
    .doOnError { logger.error("Error performing checkpoint for event $eventLogString", it) }
    .then(pipeline)
    .then()
    .onErrorResume { pipelineException ->
      val errorCategory: DeadLetterTracedQueueAsyncClient.ErrorCategory =
        if (pipelineException is NoRetryAttemptsLeftException) {
          DeadLetterTracedQueueAsyncClient.ErrorCategory.RETRY_EVENT_NO_ATTEMPTS_LEFT
        } else {
          DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR
        }
      logger.error("Exception processing event $eventLogString", pipelineException)
      deadLetterTracedQueueAsyncClient
        .sendAndTraceDeadLetterQueueEvent(
          binaryData,
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null, transactionEventCode = null, errorCategory = errorCategory))
        .then()
    }
    .then(mono {})
}

fun <T> runTracedPipelineWithDeadLetterQueue(
  checkPointer: Checkpointer,
  pipeline: Mono<T>,
  queueEvent: QueueEvent<*>,
  deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  tracingUtils: TracingUtils,
  spanName: String
): Mono<Unit> {
  val eventLogString = "${queueEvent.event.id}, transactionId: ${queueEvent.event.transactionId}"

  val deadLetterPipeline =
    checkPointer
      .success()
      .doOnSuccess { logger.info("Checkpoint performed successfully for event $eventLogString") }
      .doOnError { logger.error("Error performing checkpoint for event $eventLogString", it) }
      .then(pipeline)
      .then()
      .onErrorResume { pipelineException ->
        val errorCategory: DeadLetterTracedQueueAsyncClient.ErrorCategory =
          if (pipelineException is NoRetryAttemptsLeftException) {
            DeadLetterTracedQueueAsyncClient.ErrorCategory.RETRY_EVENT_NO_ATTEMPTS_LEFT
          } else {
            DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR
          }
        logger.error("Exception processing event $eventLogString", pipelineException)
        deadLetterTracedQueueAsyncClient
          .sendAndTraceDeadLetterQueueEvent(
            BinaryData.fromObject(queueEvent),
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(queueEvent.event.transactionId),
              transactionEventCode = queueEvent.event.eventCode,
              errorCategory = errorCategory))
          .then()
      }

  return tracingUtils
    .traceMonoWithRemoteSpan(queueEvent.tracingInfo, spanName, deadLetterPipeline)
    .then(mono {})
}

fun getClosePaymentOutcome(tx: BaseTransaction): TransactionClosureData.Outcome? =
  when (tx) {
    is BaseTransactionClosed -> tx.transactionClosureData.responseOutcome
    is BaseTransactionExpired -> getClosePaymentOutcome(tx.transactionAtPreviousState)
    else -> null
  }

fun wasClosePaymentResponseOutcomeKo(tx: BaseTransaction) =
  getClosePaymentOutcome(tx) == TransactionClosureData.Outcome.KO

fun <T> timeLeftForSendPaymentResult(
  tx: BaseTransaction,
  sendPaymentResultTimeoutSeconds: Int,
  events: Flux<TransactionEvent<T>>
): Mono<Duration> {
  val timeout = Duration.ofSeconds(sendPaymentResultTimeoutSeconds.toLong())
  val transactionStatus = tx.status
  return if (transactionStatus == TransactionStatusDto.CLOSED) {
    events
      .filter {
        TransactionEventCode.valueOf(it.eventCode) ==
          TransactionEventCode.TRANSACTION_CLOSED_EVENT &&
          (it as TransactionClosedEvent).data.responseOutcome == TransactionClosureData.Outcome.OK
      }
      .next()
      .map {
        val closePaymentDate = ZonedDateTime.parse(it.creationDate)
        val now = ZonedDateTime.now()
        val timeLeft = Duration.between(now, closePaymentDate.plus(timeout))
        logger.info("Transaction close payment done at: $closePaymentDate, time left: $timeLeft")
        return@map timeLeft
      }
  } else {
    Mono.empty()
  }
}
