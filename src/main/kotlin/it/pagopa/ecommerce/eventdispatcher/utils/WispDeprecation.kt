package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.domain.PaymentNotice
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedUserReceipt

object WispDeprecation {

  fun getPaymentNoticeId(
    transaction: BaseTransactionWithRequestedUserReceipt,
    notice: PaymentNotice
  ): String =
    when (transaction.clientId) {
      Transaction.ClientId.WISP_REDIRECT -> notice.creditorReferenceId ?: notice.rptId.noticeId
      else -> notice.rptId.noticeId
    }
}
