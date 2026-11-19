package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class BadGatewayException(detail: String, throwable: Throwable? = null) :
  RuntimeException("Bad gateway ${if (detail.isNotEmpty()) ": $detail" else ""}", throwable)
