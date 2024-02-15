package it.pagopa.ecommerce.eventdispatcher.exceptions

import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class UnauthorizedPatchAuthorizationRequestException(
  val transactionID: UUID,
  val statusCode: HttpStatus?
) :
  RuntimeException(
    "Unauthorized. Error performing patch authorization for transaction with id $transactionID. Status code: ${statusCode ?: "N/A"}")
