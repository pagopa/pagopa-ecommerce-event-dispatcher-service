package it.pagopa.ecommerce.scheduler.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus


@ResponseStatus(value = HttpStatus.BAD_GATEWAY)
class BadGatewayException(val detail: String) : RuntimeException()