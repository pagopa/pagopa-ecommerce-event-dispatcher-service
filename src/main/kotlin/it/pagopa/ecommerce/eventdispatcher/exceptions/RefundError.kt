package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.documents.v2.authorization.TransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId

sealed class RefundError(cause: Throwable?) : Exception(cause) {
  data class UnexpectedPaymentGatewayResponse(
    val transactionId: TransactionId,
    override val cause: Throwable?,
    override val message: String
  ) : RefundError(cause)

  data class RefundFailed(
    val transactionId: TransactionId,
    val authorizationData: TransactionGatewayAuthorizationData?,
    override val cause: Throwable?,
    override val message: String
  ) : RefundError(cause)
}
