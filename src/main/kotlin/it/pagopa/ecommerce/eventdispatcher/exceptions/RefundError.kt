package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient

sealed class RefundError : Exception() {
  data class UnexpectedPaymentGatewayResponse(
    val transactionId: TransactionId,
    override val message: String
  ) : RefundError()

  data class RefundFailed(
    val transactionId: TransactionId,
    val authorizationData: TransactionGatewayAuthorizationData,
    override val message: String
  ) : RefundError()
}

fun RefundError.toDeadLetterErrorCategory() =
  when (this) {
    is RefundError.UnexpectedPaymentGatewayResponse ->
      DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED
    is RefundError.RefundFailed -> TODO()
  }
