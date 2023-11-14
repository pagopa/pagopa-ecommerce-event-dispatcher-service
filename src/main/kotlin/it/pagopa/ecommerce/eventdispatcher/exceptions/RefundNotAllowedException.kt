package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus
import java.util.*

@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class RefundNotAllowedException(transactionID: UUID, cause: String = "N/A") :
    RuntimeException("Transaction with id $transactionID cannot be refunded. Reason: $cause")
