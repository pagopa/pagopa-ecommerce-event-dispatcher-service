package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class RefundNotAllowedException(detail: String) :
    RuntimeException("Refund not allowed exception ${if (detail.isNotEmpty()) ": $detail" else ""}")
