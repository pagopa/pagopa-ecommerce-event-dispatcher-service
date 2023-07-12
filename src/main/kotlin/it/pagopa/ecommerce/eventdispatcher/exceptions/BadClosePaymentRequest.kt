package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class BadClosePaymentRequest(detail: String) :
  RuntimeException("Bad closePayment request ${if (detail.isNotEmpty()) ": $detail" else ""}")
