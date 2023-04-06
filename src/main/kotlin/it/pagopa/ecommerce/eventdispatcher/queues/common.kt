package it.pagopa.ecommerce.eventdispatcher.queues

import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.queues.QueueCommonsLogger.logger
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
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
  transactionsViewRepository: TransactionsViewRepository,
  wasAuthorizationRequested: Boolean
): Mono<BaseTransaction> {

  return transactionsExpiredEventStoreRepository
    .save(
      TransactionExpiredEvent(
        transaction.transactionId.value.toString(), TransactionExpiredData(transaction.status)))
    .then(
      transactionsViewRepository.save(
        Transaction(
          transaction.transactionId.value.toString(),
          paymentNoticeDocuments(transaction.paymentNotices),
          TransactionUtils.getTransactionFee(transaction).orElse(null),
          transaction.email,
          getExpiredTransactionStatus(wasAuthorizationRequested),
          transaction.clientId,
          transaction.creationDate.toString())))
    .doOnSuccess {
      logger.info("Transaction expired for transaction ${transaction.transactionId.value}")
    }
    .doOnError {
      logger.error(
        "Transaction expired error for transaction ${transaction.transactionId.value} : ${it.message}")
    }
    .thenReturn(transaction)
}

fun getExpiredTransactionStatus(wasAuthorizationRequested: Boolean) =
  if (wasAuthorizationRequested) {
    TransactionStatusDto.EXPIRED
  } else {
    TransactionStatusDto.EXPIRED_NOT_AUTHORIZED
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
      transaction.transactionId.value.toString(), TransactionRefundedData(transaction.status)),
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
      transaction.transactionId.value.toString(), TransactionRefundedData(transaction.status)),
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
      transaction.transactionId.value.toString(), TransactionRefundedData(transaction.status)),
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
      transactionsViewRepository.save(
        Transaction(
          transaction.transactionId.value.toString(),
          paymentNoticeDocuments(transaction.paymentNotices),
          TransactionUtils.getTransactionFee(transaction).orElse(null),
          transaction.email,
          status,
          transaction.clientId,
          transaction.creationDate.toString())))
    .doOnSuccess {
      logger.info(
        "Updated event for transaction with id ${transaction.transactionId.value} to status $status")
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

      paymentGatewayClient.requestRefund(UUID.fromString(authorizationRequestId)).map {
        refundResponse ->
        Pair(refundResponse, transaction)
      }
    }
    .flatMap {
      val (refundResponse, transaction) = it
      logger.info(
        "Transaction requestRefund for transaction ${transaction.transactionId} with outcome ${refundResponse.refundOutcome}")
      when (refundResponse.refundOutcome) {
        "OK" ->
          updateTransactionToRefunded(
            transaction, transactionsEventStoreRepository, transactionsViewRepository)
        else ->
          Mono.error(
            RuntimeException(
              "Refund error for transaction ${transaction.transactionId} with outcome  ${refundResponse.refundOutcome}"))
      }
    }
    .onErrorResume { exception ->
      logger.error(
        "Transaction requestRefund error for transaction ${tx.transactionId.value} : ${exception.message}")
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

fun paymentNoticeDocuments(
  paymentNotices: List<it.pagopa.ecommerce.commons.domain.v1.PaymentNotice>
): List<PaymentNotice> {
  return paymentNotices.map { notice ->
    PaymentNotice(
      notice.paymentToken.value,
      notice.rptId.value,
      notice.transactionDescription.value,
      notice.transactionAmount.value,
      notice.paymentContextCode.value,
      notice.transferList.map { item ->
        PaymentTransferInformation(
          item.paFiscalCode, item.digitalStamp, item.transferAmount, item.transferCategory)
      })
  }
}

fun isTransactionRefundable(tx: BaseTransaction): Boolean {
  val wasAuthorizationRequested = wasAuthorizationRequested(tx)
  val wasSendPaymentResultOutcomeOK =
    if (tx is BaseTransactionWithRequestedUserReceipt) {
      tx.transactionUserReceiptData.responseOutcome == TransactionUserReceiptData.Outcome.OK
    } else {
      false
    }
  val isTransactionRefundable = wasAuthorizationRequested && !wasSendPaymentResultOutcomeOK
  logger.info(
    "Transaction with if ${tx.transactionId} : was authorization requested: $wasAuthorizationRequested, was send payment result outcome OK : $wasSendPaymentResultOutcomeOK --> is refundable: $isTransactionRefundable")
  return isTransactionRefundable
}

fun wasAuthorizationRequested(tx: BaseTransaction) = tx is BaseTransactionWithRequestedAuthorization

fun isTransactionExpired(tx: BaseTransaction): Boolean = tx.status == TransactionStatusDto.EXPIRED

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
      transaction.transactionId.value.toString(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value, newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value.toString())
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
      transaction.transactionId.value.toString(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value, newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value.toString())
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
