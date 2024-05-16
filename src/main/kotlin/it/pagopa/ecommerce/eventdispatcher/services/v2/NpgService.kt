package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationTypeDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OrderResponseDto
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidNPGResponseException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono

sealed interface NpgOrderStatus

data class UnknownNpgOrderStatus(val order: OrderResponseDto) : NpgOrderStatus

data class NgpOrderAuthorized(
  val authorization: OperationDto,
) : NpgOrderStatus

data class NpgOrderRefunded(val refundOperation: OperationDto) : NpgOrderStatus

data class NgpOrderNotAuthorized(
  val operation: OperationDto,
) : NpgOrderStatus

@Service
class NpgService(
  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService
) {

  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  fun getAuthorizationDataFromNpgOrder(
    transaction: BaseTransactionWithRequestedAuthorization
  ): Mono<TransactionGatewayAuthorizationData> {
    return getNpgOrderStatus(transaction).flatMap {
      when (it) {
        is NgpOrderNotAuthorized -> {
          logger.info(
            "Transaction with id [{}] not authorized, doing nothing", transaction.transactionId)
          Mono.empty()
        }
        is NgpOrderAuthorized ->
          it.authorization.operationId?.let { operationId ->
            NpgTransactionGatewayAuthorizationData(
                it.authorization.operationResult,
                operationId,
                it.authorization.paymentEndToEndId,
                null,
                null)
              .toMono()
          }
            ?: Mono.error(InvalidNPGResponseException())
        is NpgOrderRefunded -> {
          logger.info(
            "Unexpected order refunded for transaction with id [{}]", transaction.transactionId)
          Mono.error(InvalidNPGResponseException())
        }
        is UnknownNpgOrderStatus -> {
          logger.error(
            "Cannot establish Npg Order status for transaction [{}]", transaction.transactionId)
          Mono.error(InvalidNPGResponseException())
        }
      }
    }
  }

  fun getNpgOrderStatus(
    transaction: BaseTransactionWithRequestedAuthorization
  ): Mono<NpgOrderStatus> {
    return authorizationStateRetrieverService
      .getOrder(transaction)
      .doOnNext { order ->
        logger.info(
          "Performed get order for transaction with id: [{}], last operation result: [{}], operations: [{}]",
          transaction.transactionId,
          order.orderStatus?.lastOperationType,
          order.operations?.joinToString { "${it.operationType}-${it.operationResult}" },
        )
      }
      .flatMap {
        it.operations
          ?.fold(UnknownNpgOrderStatus(it) as NpgOrderStatus, this::reduceOperations)
          ?.toMono()
          ?: Mono.error(InvalidNPGResponseException())
      }
  }

  private fun reduceOperations(
    orderState: NpgOrderStatus,
    operation: OperationDto
  ): NpgOrderStatus =
    when {
      operation.operationType == OperationTypeDto.AUTHORIZATION &&
        operation.operationResult != OperationResultDto.EXECUTED &&
        orderState !is NpgOrderRefunded &&
        orderState !is NgpOrderAuthorized -> NgpOrderNotAuthorized(operation)
      operation.operationType == OperationTypeDto.AUTHORIZATION &&
        operation.operationResult == OperationResultDto.EXECUTED &&
        orderState !is NpgOrderRefunded -> NgpOrderAuthorized(operation)
      operation.operationType == OperationTypeDto.REFUND &&
        operation.operationResult == OperationResultDto.VOIDED -> NpgOrderRefunded(operation)
      else -> orderState
    }
}
