package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus
import java.nio.charset.StandardCharsets

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class InvalidEventException(eventPayload: ByteArray) :
    RuntimeException("Invalid input event: ${eventPayload.toString(StandardCharsets.UTF_8)}")
