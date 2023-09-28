package it.pagopa.ecommerce.eventdispatcher.exceptions

import java.nio.charset.StandardCharsets
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class InvalidEventException(eventPayload: ByteArray, cause: Throwable?) :
  RuntimeException("Invalid input event: ${eventPayload.toString(StandardCharsets.UTF_8)}", cause)
