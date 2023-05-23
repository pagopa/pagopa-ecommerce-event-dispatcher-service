package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.queues.QueueCommonsLogger.logger
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto.StatusEnum
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
      transactionsViewRepository.findByTransactionId(transaction.transactionId.value()).flatMap { tx
        ->
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
      transactionsViewRepository.findByTransactionId(transaction.transactionId.value()).flatMap { tx
        ->
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
  retryCount: Int = 0
): Mono<BaseTransaction> {
  return Mono.just(tx)
    .cast(BaseTransactionWithRequestedAuthorization::class.java)
    .flatMap { transaction ->
      val authorizationRequestId =
        transaction.transactionAuthorizationRequestData.authorizationRequestId

      when (transaction.transactionAuthorizationRequestData.paymentGateway) {
        TransactionAuthorizationRequestData.PaymentGateway.XPAY ->
          paymentGatewayClient.requestXPayRefund(UUID.fromString(authorizationRequestId)).map {
            refundResponse ->
            Pair(refundResponse, transaction)
          }
        TransactionAuthorizationRequestData.PaymentGateway.VPOS ->
          paymentGatewayClient.requestVPosRefund(UUID.fromString(authorizationRequestId)).map {
            refundResponse ->
            Pair(refundResponse, transaction)
          }
        else ->
          Mono.error(
            RuntimeException(
              "Refund error for transaction ${transaction.transactionId} - unhandled payment-gateway"))
      }
    }
    .flatMap {
      val (refundResponse, transaction) = it

      when (refundResponse) {
        is VposDeleteResponseDto ->
          handleVposRefundResponse(
            transaction,
            refundResponse,
            transactionsEventStoreRepository,
            transactionsViewRepository)
        is XPayRefundResponse200Dto ->
          handleXpayRefundResponse(
            transaction,
            refundResponse,
            transactionsEventStoreRepository,
            transactionsViewRepository)
        else ->
          Mono.error(
            RuntimeException(
              "Refund error for transaction ${transaction.transactionId}, unhandled refund response: ${refundResponse.javaClass}"))
      }
    }
    .onErrorResume { exception ->
      logger.error(
        "Transaction requestRefund error for transaction ${tx.transactionId.value()}", exception)
      if (retryCount == 0) {
          // refund error event written only the first time
          updateTransactionToRefundError(
            tx, transactionsEventStoreRepository, transactionsViewRepository)
        } else {
          Mono.just(tx)
        }
        .flatMap { refundRetryService.enqueueRetryEvent(it, retryCount) }
        .thenReturn(tx)
    }
}

fun handleVposRefundResponse(
  transaction: BaseTransactionWithRequestedAuthorization,
  refundResponse: VposDeleteResponseDto,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  logger.info(
    "Transaction requestRefund for transaction ${transaction.transactionId} PGS refund status [${refundResponse.status}]")

  return when (refundResponse.status) {
    StatusEnum.CANCELLED -> {
      logger.info(
        "Refund for transaction with id: [${transaction.transactionId.value()}] processed successfully")
      updateTransactionToRefunded(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    }
    StatusEnum.DENIED -> {
      logger.info(
        "Refund for transaction with id: [${transaction.transactionId.value()}] denied! No more attempts will be performed")
      updateTransactionToRefundError(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    }
    else ->
      Mono.error(
        RuntimeException(
          "Refund error for transaction ${transaction.transactionId} unhandled PGS response status [${refundResponse.status}]"))
  }
}

fun handleXpayRefundResponse(
  transaction: BaseTransactionWithRequestedAuthorization,
  refundResponse: XPayRefundResponse200Dto,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  logger.info(
    "Transaction requestRefund for transaction ${transaction.transactionId} PGS response status [${refundResponse.status}]")

  return when (refundResponse.status) {
    XPayRefundResponse200Dto.StatusEnum.CANCELLED -> {
      logger.info(
        "Refund for transaction with id: [${transaction.transactionId.value()}] processed successfully")
      updateTransactionToRefunded(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    }
    XPayRefundResponse200Dto.StatusEnum.DENIED -> {
      logger.info(
        "Refund for transaction with id: [${transaction.transactionId.value()}] denied! No more attempts will be performed")
      updateTransactionToRefundError(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    }
    else ->
      Mono.error(
        RuntimeException(
          "Refund error for transaction ${transaction.transactionId} unhandled PGS response status [${refundResponse.status}]"))
  }
}

fun isTransactionRefundable(tx: BaseTransaction): Boolean {
  val wasAuthorizationRequested = wasAuthorizationRequested(tx)
  val wasSendPaymentResultOutcomeOK = wasSendPaymentResultOutcomeOK(tx)
  val wasAuthorizationDenied = wasAuthorizationDenied(tx)
  val isTransactionRefundable =
    wasAuthorizationRequested && !wasSendPaymentResultOutcomeOK && !wasAuthorizationDenied
  logger.info(
    "Transaction with id ${tx.transactionId.value()} : was authorization requested: $wasAuthorizationRequested, was authorization denied: $wasAuthorizationDenied, was send payment result outcome OK : $wasSendPaymentResultOutcomeOK --> is refundable: $isTransactionRefundable")
  return isTransactionRefundable
}

fun wasSendPaymentResultOutcomeOK(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionWithRequestedUserReceipt ->
      tx.transactionUserReceiptData.responseOutcome == TransactionUserReceiptData.Outcome.OK
    is BaseTransactionExpired -> wasSendPaymentResultOutcomeOK(tx.transactionAtPreviousState)
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

fun wasAuthorizationDenied(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionWithCompletedAuthorization -> !tx.wasTransactionAuthorized()
    is TransactionWithClosureError ->
      tx
        .transactionAtPreviousState()
        .map { txAtPreviousStep ->
          txAtPreviousStep.isRight && wasAuthorizationDenied(txAtPreviousStep.get())
        }
        .orElse(false)
    is BaseTransactionExpired -> wasAuthorizationDenied(tx.transactionAtPreviousState)
    else -> false
  }

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
  transactionId
    .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it) }
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
    }
  val event =
    TransactionUserReceiptAddedEvent(
      transaction.transactionId.value(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value())
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
        refundRetryService)
    }
}

fun eventPipelineCheckpoint(
  checkPointer: Checkpointer,
  pipeline: Mono<*>,
  eventPayload: ByteArray,
  deadLetterQueueAsyncClient: QueueAsyncClient
): Mono<Void> {
  // parse the event as a TransactionActivatedEvent just to extract transactionId and event code
  val binaryData = BinaryData.fromBytes(eventPayload)
  val event = binaryData.toObject(TransactionActivatedEvent::class.java)
  val eventLogString = "${event.eventCode}, transactionId: ${event.transactionId}"
  return checkPointer
    .success()
    .doOnSuccess { logger.info("Checkpoint performed successfully for event $eventLogString") }
    .doOnError { logger.error("Error performing checkpoint for event $eventLogString", it) }
    .then(pipeline)
    .then()
    .onErrorResume { pipelineException ->
      logger.error("Exception processing event $eventLogString", pipelineException)
      deadLetterQueueAsyncClient
        .sendMessageWithResponse(
          binaryData,
          Duration.ZERO,
          null, // timeToLive
        )
        .doOnNext {
          logger.info(
            "Event: [${eventPayload.toString(StandardCharsets.UTF_8)}] successfully sent with visibility timeout: [${it.value.timeNextVisible}] ms to queue: [${deadLetterQueueAsyncClient.queueName}]")
        }
        .doOnError { queueException ->
          logger.error(
            "Error sending event: [${eventPayload.toString(StandardCharsets.UTF_8)}] to dead letter queue.",
            queueException)
        }
        .then()
    }
}
