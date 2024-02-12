package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class ClosePaymentErrorResponseException(
  val statusCode: HttpStatus?,
  val errorResponse: ErrorDto?
) :
  RuntimeException(
    "Error performing close payment. Status code: ${statusCode ?: "N/A"}, received response: ${errorResponse ?: "N/A"}")
