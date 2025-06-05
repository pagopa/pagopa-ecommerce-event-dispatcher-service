package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class UnauthorizedPatchAuthorizationRequestException(
  transactionID: TransactionId,
  val statusCode: HttpStatusCode?
) :
  RuntimeException(
    "Unauthorized. Error performing patch authorization for transaction with id ${transactionID.value()}. Status code: ${statusCode ?: "N/A"}")
