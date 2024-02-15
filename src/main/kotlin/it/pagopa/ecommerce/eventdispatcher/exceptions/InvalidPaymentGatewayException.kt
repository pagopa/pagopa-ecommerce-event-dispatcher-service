package it.pagopa.ecommerce.eventdispatcher.exceptions

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
open class InvalidPaymentGatewayException(transactionId: String, gateway: String) :
  RuntimeException(
    "Invalid transaction authorization data: transaction with id $transactionId hasn't expected gateway $gateway")
