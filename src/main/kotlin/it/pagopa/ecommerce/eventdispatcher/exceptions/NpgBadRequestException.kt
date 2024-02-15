package it.pagopa.ecommerce.eventdispatcher.exceptions

import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class NpgBadRequestException(transactionID: UUID, cause: String = "N/A") :
  RuntimeException(
    "Transaction with id $transactionID npg state cannot be retrieved. Reason: $cause")
