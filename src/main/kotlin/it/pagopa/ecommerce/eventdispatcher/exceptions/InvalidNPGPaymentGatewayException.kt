package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
class InvalidNPGPaymentGatewayException(transactionId: TransactionId) :
  InvalidPaymentGatewayException(transactionId, "NPG")
