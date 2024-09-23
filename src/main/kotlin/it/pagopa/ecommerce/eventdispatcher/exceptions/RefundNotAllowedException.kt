package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class RefundNotAllowedException(
  transactionID: TransactionId,
  errorMessage: String = "N/A",
  cause: Throwable? = null
) :
  RuntimeException(
    "Transaction with id ${transactionID.value()} cannot be refunded. Reason: $errorMessage",
    cause)
