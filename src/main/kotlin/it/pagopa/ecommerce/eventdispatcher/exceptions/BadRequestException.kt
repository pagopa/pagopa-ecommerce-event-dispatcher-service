package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class BadRequestException(detail: String) :
  RuntimeException("Bad request ${if (detail.isNotEmpty()) ": $detail" else ""}")
