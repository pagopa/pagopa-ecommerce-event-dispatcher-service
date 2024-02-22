package it.pagopa.ecommerce.eventdispatcher.exceptions

import it.pagopa.ecommerce.commons.domain.TransactionId
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
open class InvalidPaymentGatewayException(transactionId: TransactionId, gateway: String) :
  RuntimeException(
    "Invalid transaction authorization data: transaction with id ${transactionId.value()} hasn't expected gateway $gateway")
