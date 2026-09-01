package it.pagopa.ecommerce.eventdispatcher.services.v2

import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedAuthorization
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationTypeDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OrderResponseDto
import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidNPGResponseException
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidNpgOrderStateException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono

sealed interface NpgOrderStatus

data class UnknownNpgOrderStatus(val order: OrderResponseDto) : NpgOrderStatus

data class NgpOrderAuthorized(
  val authorization: OperationDto,
) : NpgOrderStatus

data class NpgOrderRefunded(
  val refundOperation: OperationDto,
  val authorization: OperationDto? = null
) : NpgOrderStatus

data class NgpOrderNotAuthorized(
  val operation: OperationDto,
) : NpgOrderStatus

data class NgpOrderPendingStatus(
  val operation: OperationDto,
) : NpgOrderStatus

@Service
class NpgService(
  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  @Value("\${npg.refund.delayFromAuthRequestMinutes}") val refundDelayFromAuthRequestMinutes: Long,
  @Value("\${npg.refund.eventProcessingDelaySeconds}") val eventProcessingDelaySeconds: Long
) {

  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  fun getAuthorizationDataFromNpgOrder(
    transaction: BaseTransactionWithRequestedAuthorization
  ): Mono<TransactionGatewayAuthorizationData> {
    return getNpgOrderStatus(transaction).flatMap { orderStatus ->
      when (orderStatus) {
        is NgpOrderNotAuthorized -> {
          LogTracingUtils.loggerTracingUtils()
            .success()
            .logInfo(logger, "Transaction not authorized, doing nothing")
          Mono.empty()
        }
        is NgpOrderAuthorized -> orderStatus.authorization.toAuthorizationData()?.toMono()
            ?: Mono.error(InvalidNPGResponseException("Missing mandatory operationId"))
        is NpgOrderRefunded -> {
          LogTracingUtils.loggerTracingUtils()
            .failure()
            .logError(logger, null, "Unexpected order refunded for transaction")
          Mono.error(
            InvalidNpgOrderStateException.OrderAlreadyRefunded(
              orderStatus.refundOperation, orderStatus.authorization?.toAuthorizationData()))
        }
        is NgpOrderPendingStatus -> {
          LogTracingUtils.loggerTracingUtils()
            .success()
            .logWarn(logger, "Received authorization PENDING status from NPG get order")
          Mono.error(InvalidNpgOrderStateException.OrderPendingStatus(orderStatus.operation))
        }
        is UnknownNpgOrderStatus -> {
          LogTracingUtils.loggerTracingUtils()
            .failure()
            .logError(logger, null, "Cannot establish Npg Order status for transaction")
          Mono.error(InvalidNpgOrderStateException.UnknownOrderStatus(orderStatus.order))
        }
      }
    }
  }

  fun getNpgOrderStatus(
    transaction: BaseTransactionWithRequestedAuthorization
  ): Mono<NpgOrderStatus> {
    return authorizationStateRetrieverService
      .performGetOrder(transaction)
      .doOnNext { order ->
        LogTracingUtils.loggerTracingUtils()
          .success()
          .dependency(LogTracingUtils.NPG_DEPENDENCY)
          .details(
            mapOf(
              "last_operation_type" to order.orderStatus?.lastOperationType.toString(),
              "operations" to
                order.operations?.joinToString { "${it.operationType}-${it.operationResult}" }))
          .logInfo(logger, "Performed get order for transaction")
      }
      .flatMap { orderResponse ->
        orderResponse.operations
          ?.fold(UnknownNpgOrderStatus(orderResponse) as NpgOrderStatus, this::reduceOperations)
          ?.toMono()
          ?.map {
            if (it is NpgOrderRefunded) {
              it.copy(authorization = orderResponse?.operations?.find(IS_AUTHORIZED))
            } else {
              it
            }
          }
          ?: Mono.error(
            InvalidNpgOrderStateException.UnknownOrderStatus(orderResponse, "No operations"))
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
        orderState !is NgpOrderAuthorized -> {
        if (operation.operationResult == OperationResultDto.PENDING) {
          NgpOrderPendingStatus(operation)
        } else {
          NgpOrderNotAuthorized(operation)
        }
      }
      IS_AUTHORIZED(operation) && orderState !is NpgOrderRefunded -> NgpOrderAuthorized(operation)
      IS_REFUNDED(operation) -> NpgOrderRefunded(operation)
      else -> orderState
    }

  private fun OperationDto.toAuthorizationData(): NpgTransactionGatewayAuthorizationData? =
    operationId?.let {
      NpgTransactionGatewayAuthorizationData(
        this.operationResult, it, this.paymentEndToEndId, null, null)
    }

  companion object {
    private val IS_AUTHORIZED: (OperationDto) -> Boolean = {
      it.operationType == OperationTypeDto.AUTHORIZATION &&
        it.operationResult == OperationResultDto.EXECUTED
    }
    private val IS_REFUNDED: (OperationDto) -> Boolean = {
      it.operationType == OperationTypeDto.REFUND && it.operationResult == OperationResultDto.VOIDED
    }
  }
}
