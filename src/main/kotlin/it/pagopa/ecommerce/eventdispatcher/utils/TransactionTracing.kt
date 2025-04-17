package it.pagopa.ecommerce.eventdispatcher.utils

class TransactionTracing {
  companion object {
    const val TRANSACTIONID: String = "eCommerce.transactionId"
    const val TRANSACTIONEVENT: String = "eCommerce.transactionEvent"
    const val TRANSACTIONSTATUS: String = "eCommerce.transactionStatus"
    const val PSPID: String = "eCommerce.pspId"
    const val CLIENTID: String = "eCommerce.clientId"
    const val PAYMENTMETHOD: String = "eCommerce.paymentMethod"
    // From activated datetime to the final status datetime, in milliseconds
    const val TRANSACTIONTOTALTIME: String = "eCommerce.transactionLifecycleTime"
    // From authorization requested datetime to authorization completed, in milliseconds
    const val TRANSACTIONAUTHORIZATIONTIME: String = "eCommerce.transactionAuthorizationProcessTime"
    // From close payment request to add user receipt response, in milliseconds
    const val TRANSACTIONCLOSEPAYMENTTOUSERRECEIPTTIME: String =
      "eCommerce.transactionClosePaymentToUserReceiptTime"
  }
}
