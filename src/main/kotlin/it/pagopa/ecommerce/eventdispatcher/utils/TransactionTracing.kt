package it.pagopa.ecommerce.eventdispatcher.utils

class TransactionTracing {
  companion object {
    const val TRANSACTIONID: String = "eCommerce.transactionId"
    const val TRANSACTIONEVENT: String = "eCommerce.transactionEvent"
    const val TRANSACTIONSTATUS: String = "eCommerce.transactionStatus"
    const val PSPID: String = "eCommerce.pspId"
    const val CLIENTID: String = "eCommerce.clientId"
    const val PAYMENTMETHOD: String = "eCommerce.paymentMethod"
  }
}
