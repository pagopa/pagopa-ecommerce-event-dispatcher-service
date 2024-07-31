package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.activation.EmptyTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import java.time.ZonedDateTime
import org.junit.Test
import org.junit.jupiter.api.Assertions.*

class WispDeprecationTest {

  @Test
  fun `when transaction is performed by CHECKOUT_WISP client should get creditor reference id`() {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          Transaction.ClientId.CHECKOUT_CART_WISP))
    val baseTransaction =
      TransactionTestUtils.reduceEvents(*events.toTypedArray())
        as BaseTransactionWithRequestedUserReceipt
    val noticeId =
      WispDeprecation.getPaymentNoticeId(baseTransaction, baseTransaction.paymentNotices.first())
    assertEquals(noticeId, baseTransaction.paymentNotices.first().creditorReferenceId)
  }

  @Test
  fun `when transaction is performed by CHECKOUT client should get notice id from rpt`() {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          Transaction.ClientId.CHECKOUT,
        ))
    val baseTransaction =
      TransactionTestUtils.reduceEvents(*events.toTypedArray())
        as BaseTransactionWithRequestedUserReceipt
    val noticeId =
      WispDeprecation.getPaymentNoticeId(baseTransaction, baseTransaction.paymentNotices.first())
    assertEquals(noticeId, baseTransaction.paymentNotices.first().rptId.noticeId)
  }

  @Test
  fun `when transaction is performed by CHECKOUT client and creditor reference id is null should get notice id from rpt`() {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          Transaction.ClientId.CHECKOUT,
          null))
    val baseTransaction =
      TransactionTestUtils.reduceEvents(*events.toTypedArray())
        as BaseTransactionWithRequestedUserReceipt
    val noticeId =
      WispDeprecation.getPaymentNoticeId(baseTransaction, baseTransaction.paymentNotices.first())
    assertEquals(noticeId, baseTransaction.paymentNotices.first().rptId.noticeId)
  }

  private fun buildTransactionWithUserReceipt(
    activationEvent: TransactionActivatedEvent
  ): List<TransactionEvent<*>> {
    return listOf<TransactionEvent<*>>(
      activationEvent as TransactionEvent<*>,
      TransactionTestUtils.transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.NPG,
        TransactionTestUtils.npgTransactionGatewayAuthorizationRequestedData())
        as TransactionEvent<*>,
      TransactionTestUtils.transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEndToEndId", null, null))
        as TransactionEvent<*>,
      TransactionTestUtils.transactionClosureRequestedEvent() as TransactionEvent<*>,
      TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)
        as TransactionEvent<*>,
      TransactionTestUtils.transactionClosureRequestedEvent() as TransactionEvent<*>,
      TransactionTestUtils.transactionUserReceiptRequestedEvent(
        TransactionTestUtils.transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
    )
  }
}
