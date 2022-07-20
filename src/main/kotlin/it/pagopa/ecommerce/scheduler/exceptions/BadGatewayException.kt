package it.pagopa.ecommerce.scheduler.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus


@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class BadGatewayException(detail: String) : RuntimeException("Bad gateway ${if (detail.isNotEmpty()) ": $detail" else ""}")