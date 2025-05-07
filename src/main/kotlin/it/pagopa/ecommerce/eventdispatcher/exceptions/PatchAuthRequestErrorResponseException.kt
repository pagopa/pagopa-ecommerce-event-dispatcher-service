package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class PatchAuthRequestErrorResponseException(
  val transactionId: TransactionId,
  val statusCode: HttpStatus,
  val errorMessage: String?
) :
  RuntimeException(
    "Error performing patch authorization for transaction with id ${transactionId.value()}. Status code: $statusCode, received response: ${errorMessage ?: "N/A"}")
