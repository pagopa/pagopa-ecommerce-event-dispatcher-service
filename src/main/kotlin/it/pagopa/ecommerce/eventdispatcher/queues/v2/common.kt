package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.*
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.StateResponseDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.WorkflowStateDto
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.QueueCommonsLogger.logger
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto.StatusEnum
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundOutcomeDto
import it.pagopa.generated.transactionauthrequests.v1.dto.OutcomeNpgGatewayDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.math.BigDecimal
import java.time.*
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono

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

fun retrieveAuthorizationState(
  trx: BaseTransaction,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService
): Mono<StateResponseDto> {
  return Mono.just(trx)
    .cast(BaseTransactionWithRequestedAuthorization::class.java)
    .filter { transaction ->
      transaction.transactionAuthorizationRequestData.paymentGateway ==
        TransactionAuthorizationRequestData.PaymentGateway.NPG
    }
    .switchIfEmpty(Mono.error(InvalidNPGPaymentGatewayException(trx.transactionId)))
    .flatMap { transaction ->
      authorizationStateRetrieverService.getStateNpg(
        transactionId = transaction.transactionId,
        sessionId =
          retrieveGetStateSessionId(
            transaction.transactionAuthorizationRequestData
              .transactionGatewayAuthorizationRequestedData
              as NpgTransactionGatewayAuthorizationRequestedData),
        pspId = transaction.transactionAuthorizationRequestData.pspId,
        correlationId =
          (transaction.transactionActivatedData.transactionGatewayActivationData
              as NpgTransactionGatewayActivationData)
            .correlationId,
        paymentMethod =
          NpgClient.PaymentMethod.valueOf(
            transaction.transactionAuthorizationRequestData.paymentMethodName))
    }
}

fun handleGetStateByPatchTransactionService(
  tx: BaseTransaction,
  authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  transactionsServiceClient: TransactionsServiceClient,
  tracingInfo: TracingInfo,
  retryCount: Int = 0
): Mono<BaseTransaction> {
  return retrieveAuthorizationState(tx, authorizationStateRetrieverService)
    .flatMap { stateResponseDto ->
      patchAuthRequestByState(
        stateResponseDto = stateResponseDto,
        tx = tx,
        transactionsServiceClient = transactionsServiceClient)
    }
    .thenReturn(tx)
    .onErrorResume { exception ->
      logger.error(
        "Transaction handleGetState error for transaction ${tx.transactionId.value()}", exception)
      Mono.just(tx)
        .flatMap {
          when (exception) {
            // Enqueue retry event only if getState is 5xx or 2xx with no PAYMENT_COMPLETE or
            // patchRequest is 5xx
            is NpgBadRequestException, // 400 from NPG
            is TransactionNotFound, // 404 from transactions-service
            is UnauthorizedPatchAuthorizationRequestException, // 401 from transactions-service
            is PatchAuthRequestErrorResponseException, // 400 from transactions-service
            is InvalidNPGPaymentGatewayException, // 400 from NPG
            -> Mono.empty() //
            else ->
              authorizationStateRetrieverRetryService
                .enqueueRetryEvent(tx, retryCount, tracingInfo)
                .onErrorResume { enqueueException ->
                  logger.error(
                    "Transaction enqueue retry event error for transaction ${tx.transactionId.value()}",
                    enqueueException)
                  Mono.just(tx).flatMap {
                    when (enqueueException) {
                      is TooLateRetryAttemptException,
                      is NoRetryAttemptsLeftException, -> Mono.empty()
                      else -> Mono.error(enqueueException)
                    }
                  }
                }
          }
        }
        .thenReturn(tx)
    }
}

fun retrieveGetStateSessionId(
  authRequestedGatewayData: NpgTransactionGatewayAuthorizationRequestedData
): String {
  val sessionId = authRequestedGatewayData.sessionId
  val confirmPaymentSessionId = authRequestedGatewayData.confirmPaymentSessionId
  val sessionIdToUse = Optional.ofNullable(confirmPaymentSessionId).orElse(sessionId)
  logger.info(
    "NPG authorization request sessionId: [{}], confirm payment session id: [{}] -> session id to use for retrieve state: [{}]",
    sessionId,
    confirmPaymentSessionId,
    sessionIdToUse)
  return sessionIdToUse
}

fun patchAuthRequestByState(
  stateResponseDto: StateResponseDto,
  tx: BaseTransaction,
  transactionsServiceClient: TransactionsServiceClient,
): Mono<TransactionInfoDto> {
  logger.info(
    "NPG Get State for transaction with id: [{}] processed successfully with state result [{}]",
    tx.transactionId.value(),
    stateResponseDto.state?.value ?: "N/A")
  // invoke transaction service patch
  // TODO: refactor point
  return Mono.just(stateResponseDto)
    .filter { s ->
      s.operation != null &&
        s.operation!!.operationTime != null &&
        s.operation!!.operationResult != null
    }
    .switchIfEmpty(Mono.error(InvalidNPGResponseException()))
    .filter { s -> s.state == WorkflowStateDto.PAYMENT_COMPLETE }
    .switchIfEmpty(
      Mono.error(
        NpgPaymentGatewayStateException(
          transactionID = tx.transactionId, stateResponseDto.state?.value)))
    .map { tx }
    .flatMap { t ->
      transactionsServiceClient
        .patchAuthRequest(
          t.transactionId,
          UpdateAuthorizationRequestDto().apply {
            outcomeGateway =
              OutcomeNpgGatewayDto().apply {
                paymentGatewayType = "NPG"
                operationResult =
                  OutcomeNpgGatewayDto.OperationResultEnum.valueOf(
                    stateResponseDto.operation!!.operationResult!!.value)
                orderId = stateResponseDto.operation!!.orderId
                operationId = stateResponseDto.operation!!.operationId
                if (stateResponseDto.operation!!.additionalData != null) {
                  authorizationCode =
                    stateResponseDto.operation!!.additionalData!!["authorizationCode"] as String?
                  rrn = stateResponseDto.operation!!.additionalData!!["rrn"] as String?
                }
                paymentEndToEndId = stateResponseDto.operation!!.paymentEndToEndId
              }
            timestampOperation = getTimeStampOperation(stateResponseDto.operation!!.operationTime!!)
          })
        .doOnNext { patchResponse ->
          logger.info(
            "Transactions service PATCH authRequest for transaction with id: [{}] processed successfully. New state for transaction is [{}]",
            tx.transactionId.value(),
            patchResponse.status)
        }
    }
}

fun getTimeStampOperation(operationTime: String): OffsetDateTime {
  val operationTimeT: String = operationTime.replace(" ", "T")
  val localDateTime = LocalDateTime.parse(operationTimeT)
  val zonedDateTime = localDateTime.atZone(ZoneId.of("CET"))
  return zonedDateTime.toOffsetDateTime()
}

fun refundTransaction(
  tx: BaseTransaction,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  paymentGatewayClient: PaymentGatewayClient,
  refundService: RefundService,
  refundRetryService: RefundRetryService,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  tracingInfo: TracingInfo?,
  retryCount: Int = 0
): Mono<BaseTransaction> {
  val appendRefundedRequestEvent =
    if (tx !is BaseTransactionWithRefundRequested) {
      updateTransactionToRefundRequested(
        tx, transactionsEventStoreRepository, transactionsViewRepository)
    } else {
      Mono.just(tx)
    }

  return Mono.just(tx)
    .cast(BaseTransactionWithRequestedAuthorization::class.java)
    .flatMap { transaction ->
      val authorizationRequestId =
        transaction.transactionAuthorizationRequestData.authorizationRequestId
      when (transaction.transactionAuthorizationRequestData.paymentGateway) {
        TransactionAuthorizationRequestData.PaymentGateway.XPAY ->
          appendRefundedRequestEvent
            .flatMap {
              paymentGatewayClient.requestXPayRefund(UUID.fromString(authorizationRequestId))
            }
            .map { refundResponse -> Pair(refundResponse, transaction) }
        TransactionAuthorizationRequestData.PaymentGateway.VPOS ->
          appendRefundedRequestEvent
            .flatMap {
              paymentGatewayClient.requestVPosRefund(UUID.fromString(authorizationRequestId))
            }
            .map { refundResponse -> Pair(refundResponse, transaction) }
        TransactionAuthorizationRequestData.PaymentGateway.NPG -> {
          getAuthorizationCompletedData(transaction, authorizationStateRetrieverService)
            .onErrorResume { exception ->
              appendRefundedRequestEvent.flatMap {
                when (exception) {
                  is InvalidNPGResponseException,
                  is NpgBadRequestException ->
                    Mono.error(
                      RefundError.UnexpectedPaymentGatewayResponse(
                        tx.transactionId,
                        "Failed to get authorization data from NPG payment gateway"))
                  else -> Mono.error(exception)
                }
              }
            }
            .flatMap { authorizationData ->
              appendRefundedRequestEvent
                .map { authorizationData }
                .cast(NpgTransactionGatewayAuthorizationData::class.java)
                .flatMap {
                  refundService
                    .requestNpgRefund(
                      operationId = it.operationId,
                      idempotenceKey = transaction.transactionId.uuid,
                      amount =
                        BigDecimal(transaction.transactionAuthorizationRequestData.amount)
                          .add(BigDecimal(transaction.transactionAuthorizationRequestData.fee)),
                      pspId = transaction.transactionAuthorizationRequestData.pspId,
                      correlationId =
                        (transaction.transactionActivatedData.transactionGatewayActivationData
                            as NpgTransactionGatewayActivationData)
                          .correlationId,
                      paymentMethod =
                        NpgClient.PaymentMethod.valueOf(
                          transaction.transactionAuthorizationRequestData.paymentMethodName))
                    .map { refundResponse -> Pair(refundResponse, transaction) }
                }
            }
        }
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT -> {
          appendRefundedRequestEvent
            .flatMap {
              refundService.requestRedirectRefund(
                transactionId = transaction.transactionId,
                pspTransactionId =
                  transaction.transactionAuthorizationRequestData.authorizationRequestId,
                paymentTypeCode = transaction.transactionAuthorizationRequestData.paymentTypeCode,
                pspId = transaction.transactionAuthorizationRequestData.pspId)
            }
            .map { refundResponse -> Pair(refundResponse, transaction) }
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
        is RefundResponseDto ->
          handleNpgRefundResponse(
            transaction,
            refundResponse,
            transactionsEventStoreRepository,
            transactionsViewRepository)
        is it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto ->
          handleRedirectRefundResponse(
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
        .flatMap {
          when (exception) {
            // Enqueue retry event only if refund is allowed
            is RefundNotAllowedException -> Mono.error(exception)
            is RefundError -> Mono.error(exception)
            else -> refundRetryService.enqueueRetryEvent(it, retryCount, tracingInfo)
          }
        }
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
        "Refund for transaction with id: [${transaction.transactionId.value()}] denied with pgs response status: [${refundResponse.status}]! No more attempts will be performed")
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
        "Refund for transaction with id: [${transaction.transactionId.value()}] denied with pgs response status: [${refundResponse.status}]!! No more attempts will be performed")
      updateTransactionToRefundError(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    }
    else ->
      Mono.error(
        RuntimeException(
          "Refund error for transaction ${transaction.transactionId} unhandled PGS response status [${refundResponse.status}]"))
  }
}

fun handleNpgRefundResponse(
  transaction: BaseTransactionWithRequestedAuthorization,
  refundResponse: RefundResponseDto,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  logger.info(
    "Refund for transaction with id: [{}] and NPG operationId [{}] processed successfully",
    transaction.transactionId.value(),
    refundResponse.operationId ?: "N/A")
  return updateTransactionToRefunded(
    transaction, transactionsEventStoreRepository, transactionsViewRepository)
}

fun handleRedirectRefundResponse(
  transaction: BaseTransactionWithRequestedAuthorization,
  refundResponse: it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<TransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  val refundOutcome = refundResponse.outcome
  logger.info(
    "Refund for redirect transaction for psp: [{}] with id: [{}] processed successfully. Received outcome: [{}]",
    transaction.transactionAuthorizationRequestData.pspId,
    transaction.transactionId.value(),
    refundOutcome)
  return when (refundOutcome) {
    RefundOutcomeDto.OK,
    RefundOutcomeDto.CANCELED ->
      updateTransactionToRefunded(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    RefundOutcomeDto.KO ->
      updateTransactionToRefundError(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
  }
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

fun getGatewayAuthorizationOutcome(
  gatewayAuthorizationData: TransactionGatewayAuthorizationData
): AuthorizationResultDto {
  return when (gatewayAuthorizationData) {
    is NpgTransactionGatewayAuthorizationData ->
      if (gatewayAuthorizationData.operationResult == OperationResultDto.EXECUTED) {
        AuthorizationResultDto.OK
      } else {
        AuthorizationResultDto.KO
      }
    is PgsTransactionGatewayAuthorizationData -> gatewayAuthorizationData.authorizationResultDto
    is RedirectTransactionGatewayAuthorizationData ->
      if (gatewayAuthorizationData.outcome ==
        RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
        AuthorizationResultDto.OK
      } else {
        AuthorizationResultDto.KO
      }
  }
}

fun getAuthorizationOutcome(tx: BaseTransaction): AuthorizationResultDto? =
  when (tx) {
    is BaseTransactionWithCompletedAuthorization ->
      getGatewayAuthorizationOutcome(
        tx.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData)
    is TransactionWithClosureError -> getAuthorizationOutcome(tx.transactionAtPreviousState)
    is BaseTransactionExpired -> getAuthorizationOutcome(tx.transactionAtPreviousState)
    else -> null
  }

fun getAuthorizationCompletedData(
  tx: BaseTransaction,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService
): Mono<TransactionGatewayAuthorizationData> =
  when (tx) {
    is BaseTransactionWithCompletedAuthorization ->
      tx.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData.toMono()
    is BaseTransactionWithRefundRequested ->
      getAuthorizationCompletedData(
        tx.transactionAtPreviousState, authorizationStateRetrieverService)
    is TransactionWithClosureError ->
      getAuthorizationCompletedData(
        tx.transactionAtPreviousState, authorizationStateRetrieverService)
    is BaseTransactionExpired ->
      getAuthorizationCompletedData(
        tx.transactionAtPreviousState, authorizationStateRetrieverService)
    is BaseTransactionWithRequestedAuthorization ->
      if (tx.transactionAuthorizationRequestData.paymentGateway ==
        TransactionAuthorizationRequestData.PaymentGateway.NPG) {
        getNpgTransactionAuthorizationData(tx, authorizationStateRetrieverService)
      } else Mono.empty()
    else -> Mono.empty()
  }

private fun getNpgTransactionAuthorizationData(
  trx: BaseTransaction,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService
): Mono<TransactionGatewayAuthorizationData> {
  return retrieveAuthorizationState(trx, authorizationStateRetrieverService).flatMap { stateResponse
    ->
    val operation = stateResponse.operation
    val operationResult = operation?.operationResult
    val operationId = operation?.operationId
    logger.info(
      "Performed get state for transaction with id: [{}], received operations state: [{}], operation result: [{}] and operation id: [{}]",
      trx.transactionId.value(),
      stateResponse.state,
      operationResult,
      operationId)
    when {
      operationResult == OperationResultDto.EXECUTED ->
        operationId?.let {
          NpgTransactionGatewayAuthorizationData(
              operationResult, it, operation.paymentEndToEndId, null)
            .toMono()
        }
          ?: Mono.error(InvalidNPGResponseException())
      else -> Mono.empty()
    }
  }
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
): Mono<BaseTransaction> =
  reduceEvents(transactionId, transactionsEventStoreRepository, EmptyTransaction())

fun reduceEvents(
  transactionId: Mono<String>,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  emptyTransaction: EmptyTransaction
): Mono<BaseTransaction> =
  reduceEvents(
    transactionId.flatMapMany {
      transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(it).map {
        it as TransactionEvent<Any>
      }
    },
    emptyTransaction)

fun <T> reduceEvents(events: Flux<TransactionEvent<T>>): Mono<BaseTransaction> =
  reduceEvents(events, EmptyTransaction())

fun <T> reduceEvents(
  events: Flux<TransactionEvent<T>>,
  emptyTransaction: EmptyTransaction
): Mono<BaseTransaction> =
  events
    .reduce(emptyTransaction, it.pagopa.ecommerce.commons.domain.v2.Transaction::applyEvent)
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
  refundService: RefundService,
  refundRetryService: RefundRetryService,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  tracingInfo: TracingInfo?,
): Mono<BaseTransaction> {
  val userReceiptOutcome = transaction.transactionUserReceiptData.responseOutcome
  val toBeRefunded = userReceiptOutcome == TransactionUserReceiptData.Outcome.KO
  logger.info(
    "Transaction Nodo sendPaymentResult response outcome: $userReceiptOutcome --> to be refunded: $toBeRefunded")
  return Mono.just(transaction)
    .filter { toBeRefunded }
    .flatMap {
      refundTransaction(
        transaction,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        paymentGatewayClient,
        refundService,
        refundRetryService,
        authorizationStateRetrieverService,
        tracingInfo,
      )
    }
}

fun <T> runTracedPipelineWithDeadLetterQueue(
  checkPointer: Checkpointer,
  pipeline: Mono<T>,
  queueEvent: QueueEvent<*>,
  deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  tracingUtils: TracingUtils,
  spanName: String,
  jsonSerializerProviderV2: StrictJsonSerializerProvider
): Mono<Unit> {
  val eventLogString = "${queueEvent.event.id}, transactionId: ${queueEvent.event.transactionId}"

  val deadLetterPipeline =
    checkPointer
      .success()
      .doOnSuccess { logger.info("Checkpoint performed successfully for event $eventLogString") }
      .doOnError { logger.error("Error performing checkpoint for event $eventLogString", it) }
      .then(pipeline)
      .then(Mono.just(Unit))
      .onErrorResume { pipelineException ->
        val errorCategory: DeadLetterTracedQueueAsyncClient.ErrorCategory =
          when (pipelineException) {
            is NoRetryAttemptsLeftException ->
              DeadLetterTracedQueueAsyncClient.ErrorCategory.RETRY_EVENT_NO_ATTEMPTS_LEFT
            is RefundError.UnexpectedPaymentGatewayResponse ->
              pipelineException.toDeadLetterErrorCategory()
            else -> DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR
          }
        logger.error("Exception processing event $eventLogString", pipelineException)
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
          binaryData = BinaryData.fromObject(queueEvent, jsonSerializerProviderV2.createInstance()),
          errorContext =
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(queueEvent.event.transactionId),
              transactionEventCode = queueEvent.event.eventCode,
              errorCategory = errorCategory),
        )
      }

  return tracingUtils
    .traceMonoWithRemoteSpan(queueEvent.tracingInfo, spanName, deadLetterPipeline)
    .then(Mono.just(Unit))
}

private fun RefundError.toDeadLetterErrorCategory() =
  when (this) {
    is RefundError.UnexpectedPaymentGatewayResponse ->
      DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED
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

fun isRefundableCheckRequired(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionExpired -> isRefundableCheckRequired(tx.transactionAtPreviousState)
    is BaseTransactionWithClosureError -> isRefundableCheckRequired(tx.transactionAtPreviousState)
    else ->
      setOf(TransactionStatusDto.AUTHORIZATION_COMPLETED, TransactionStatusDto.CLOSURE_REQUESTED)
        .contains(tx.status) && !wasAuthorizationDenied(tx)
  }
