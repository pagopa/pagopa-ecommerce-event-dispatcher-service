package it.pagopa.ecommerce.scheduler.queues

import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.scheduler.client.PaymentGatewayClient
import it.pagopa.ecommerce.scheduler.queues.QueueCommonsLogger.logger
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
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
        transaction.transactionId.value.toString(), TransactionExpiredData(transaction.status)))
    .then(
      transactionsViewRepository.save(
        Transaction(
          transaction.transactionId.value.toString(),
          paymentNoticeDocuments(transaction.paymentNotices),
          TransactionUtils.getTransactionFee(transaction).orElse(null),
          transaction.email,
          TransactionStatusDto.EXPIRED,
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
  paymentGatewayClient: PaymentGatewayClient
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
    .doOnError {
      logger.error("Exception processing refund for transaction ${tx.transactionId}", it)
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
      notice.paymentContextCode.value)
  }
}

fun isTransactionRefundable(tx: BaseTransaction): Boolean =
  tx is BaseTransactionWithRequestedAuthorization

fun reduceEvents(
  transactionId: Mono<String>,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
) =
  transactionId
    .flatMapMany { transactionsEventStoreRepository.findByTransactionId(it) }
    .reduce(EmptyTransaction(), it.pagopa.ecommerce.commons.domain.v1.Transaction::applyEvent)
    .cast(BaseTransaction::class.java)
