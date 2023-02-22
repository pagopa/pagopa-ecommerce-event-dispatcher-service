package it.pagopa.ecommerce.scheduler.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus
import java.util.*

@ResponseStatus(value = HttpStatus.GATEWAY_TIMEOUT)
class TransactionNotFound(val transactionID: UUID): RuntimeException("Transaction with id $transactionID not found!")
