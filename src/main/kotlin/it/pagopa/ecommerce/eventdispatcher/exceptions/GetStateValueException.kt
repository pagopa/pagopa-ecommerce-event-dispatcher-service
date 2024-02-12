package it.pagopa.ecommerce.eventdispatcher.exceptions

import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class GetStateValueException(transactionID: UUID, state: String = "N/A") :
  RuntimeException("Transaction with id $transactionID npg state is $state")
