package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.generated.npg.v1.dto.*
import java.time.ZonedDateTime
import java.util.*
import reactor.kotlin.core.publisher.toMono

fun refundOperationId(operationId: String): String = "refund-${operationId}"

fun npgAuthorizedOrderResponse(operationId: String, paymentEndToEndId: String) =
  OrderResponseDto()
    .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))
    .addOperationsItem(npgAuthorizedOperation(operationId, paymentEndToEndId))
    .toMono()

fun npgAuthorizedOperation(operationId: String, paymentEndToEndId: String) =
  OperationDto()
    .operationId(operationId)
    .orderId(UUID.randomUUID().toString())
    .operationType(OperationTypeDto.AUTHORIZATION)
    .operationResult(OperationResultDto.EXECUTED)
    .paymentEndToEndId(paymentEndToEndId)
    .operationTime(ZonedDateTime.now().toString())

fun npgRefundOperation(operationId: String, paymentEndToEndId: String) =
  OperationDto()
    .orderId(operationId)
    .operationType(OperationTypeDto.REFUND)
    .operationResult(OperationResultDto.VOIDED)
    .paymentEndToEndId(paymentEndToEndId)
    .operationTime(ZonedDateTime.now().toString())
