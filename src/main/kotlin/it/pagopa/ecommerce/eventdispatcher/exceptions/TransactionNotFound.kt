package it.pagopa.ecommerce.eventdispatcher.exceptions

import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.GATEWAY_TIMEOUT)
class TransactionNotFound(val transactionID: UUID) :
  RuntimeException("Transaction with id $transactionID not found!")
