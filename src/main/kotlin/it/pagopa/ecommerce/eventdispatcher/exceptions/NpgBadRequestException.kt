package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class NpgBadRequestException(transactionID: TransactionId, cause: String = "N/A") :
  RuntimeException(
    "Transaction with id ${transactionID.value()} npg state cannot be retrieved. Reason: $cause")
