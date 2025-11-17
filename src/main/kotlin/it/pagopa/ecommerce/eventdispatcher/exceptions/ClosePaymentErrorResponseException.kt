package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.generated.ecommerce.nodo.v2.dto.ErrorDto
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class ClosePaymentErrorResponseException(
  val statusCode: HttpStatusCode?,
  val errorResponse: ErrorDto?
) :
  RuntimeException(
    "Error performing close payment. Status code: ${statusCode ?: "N/A"}, received response: ${errorResponse ?: "N/A"}") {

  companion object {
    const val NODE_DID_NOT_RECEIVE_RPT_YET_ERROR = "Node did not receive RPT yet"
    const val INVALID_TOKEN = "Invalid token"
  }

  /**
   * Perform check against Node received HTTP response code and description to verify if refund is
   * allowed for the current transaction or not. See
   * https://pagopa.atlassian.net/wiki/spaces/I/pages/1196130326/eCommerce+-+Messaggi+in+fase+closePayment
   * for more info
   */
  fun isRefundableError() =
    // transaction is refundable iff
    when (statusCode) {
      // http status code == 400 and description != Invalid token
      HttpStatus.BAD_REQUEST -> errorResponse?.description != INVALID_TOKEN
      // http status code == 422 and description == Node did not receive RPT yet
      HttpStatus.UNPROCESSABLE_ENTITY ->
        errorResponse?.description == NODE_DID_NOT_RECEIVE_RPT_YET_ERROR
      // http status code == 404 with any description
      HttpStatus.NOT_FOUND -> true
      // no refund is done in all other cases
      else -> false
    }
}
