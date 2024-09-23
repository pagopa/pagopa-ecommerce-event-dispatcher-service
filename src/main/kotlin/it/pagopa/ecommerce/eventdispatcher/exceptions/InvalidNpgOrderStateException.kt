package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationDto
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OrderResponseDto

sealed class InvalidNpgOrderStateException : Exception() {
  data class OrderAlreadyRefunded(
    val refundOperation: OperationDto,
    val authorizationData: TransactionGatewayAuthorizationData?,
    override val message: String? = "Order already refunded"
  ) : InvalidNpgOrderStateException()

  data class UnknownOrderStatus(
    val order: OrderResponseDto,
    override val message: String? = "Unknown NPG order status"
  ) : InvalidNpgOrderStateException()

  data class OrderPendingStatus(
    val order: OperationDto,
    override val message: String? = "Order with pending status received"
  ) : InvalidNpgOrderStateException()
}
