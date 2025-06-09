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
  }

  /**
   * Perform check against Node received HTTP response code and description to verify if refund is
   * allowed for the current transaction or not. See https://pagopa.atlassian.net/browse/CHK-3553
   * for more info
   */
  fun isRefundableError() =
    // transaction is refundable iff
    // http status code == 422 and description == Node did not receive RPT yet
    if (statusCode == HttpStatus.UNPROCESSABLE_ENTITY) {
      errorResponse?.description == NODE_DID_NOT_RECEIVE_RPT_YET_ERROR
    } else {
      // ...or http status code is 400 || 404 with any description
      statusCode == HttpStatus.NOT_FOUND || statusCode == HttpStatus.BAD_REQUEST
    }
}
