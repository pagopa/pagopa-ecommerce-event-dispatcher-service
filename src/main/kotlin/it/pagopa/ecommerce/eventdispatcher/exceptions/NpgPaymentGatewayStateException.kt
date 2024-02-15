package it.pagopa.ecommerce.eventdispatcher.exceptions

import java.util.*

class NpgPaymentGatewayStateException(transactionID: UUID, state: String = "N/A") :
  RuntimeException("Transaction with id $transactionID npg state is $state")
