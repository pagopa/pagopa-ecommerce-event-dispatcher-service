package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId
import java.util.*
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class UnauthorizedPatchAuthorizationRequestException(
  transactionID: TransactionId,
  val statusCode: HttpStatus?
) :
  RuntimeException(
    "Unauthorized. Error performing patch authorization for transaction with id ${transactionID.value()}. Status code: ${statusCode ?: "N/A"}")
