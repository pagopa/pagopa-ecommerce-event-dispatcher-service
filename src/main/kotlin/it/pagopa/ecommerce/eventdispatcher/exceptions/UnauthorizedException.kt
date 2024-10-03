package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.UNAUTHORIZED)
class UnauthorizedException(detail: String) :
  RuntimeException("Unauthorized ${if (detail.isNotEmpty()) ": $detail" else ""}")
