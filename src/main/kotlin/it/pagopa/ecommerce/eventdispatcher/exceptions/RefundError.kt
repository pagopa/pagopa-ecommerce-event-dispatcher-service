package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId

sealed class RefundError : Exception() {
  data class UnexpectedPaymentGatewayResponse(
    val transactionId: TransactionId,
    override val message: String
  ) : RefundError()
}
