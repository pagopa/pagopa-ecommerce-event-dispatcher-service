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
    "Error performing close payment. Status code: ${statusCode ?: "N/A"}, received response: ${errorResponse ?: "N/A"}") {

  companion object {
    const val NODE_DID_NOT_RECEIVE_RPT_YET_ERROR = "Node did not receive RPT yet"
    const val UNACCEPTABLE_OUTCOME_TOKEN_EXPIRED = "Unacceptable outcome when token has expired"
  }

  // transaction can be refund only for HTTP status code 422 and error response description
  // equals to "Node did not receive RPT yet" OR HTTP status code 400 and error response
  // description equal to
  // "Unacceptable outcome when token has expired"
  fun isRefundableError(): Boolean {
    return (statusCode == HttpStatus.UNPROCESSABLE_ENTITY &&
      errorResponse?.description == NODE_DID_NOT_RECEIVE_RPT_YET_ERROR) ||
      (statusCode == HttpStatus.BAD_REQUEST &&
        errorResponse?.description == UNACCEPTABLE_OUTCOME_TOKEN_EXPIRED)
  }
}
