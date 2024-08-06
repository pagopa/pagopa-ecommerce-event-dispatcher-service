package it.pagopa.ecommerce.eventdispatcher.utils

import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.Transaction.ClientId
import it.pagopa.ecommerce.commons.documents.v2.activation.EmptyTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import java.time.ZonedDateTime
import org.junit.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class WispDeprecationTest {

  @Test
  fun `when transaction is performed by WISP_REDIRECT client should get creditor reference id`() {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          ClientId.WISP_REDIRECT))
    val baseTransaction =
      TransactionTestUtils.reduceEvents(*events.toTypedArray())
        as BaseTransactionWithRequestedUserReceipt
    val noticeId =
      WispDeprecation.getPaymentNoticeId(baseTransaction, baseTransaction.paymentNotices.first())
    assertEquals(noticeId, baseTransaction.paymentNotices.first().creditorReferenceId)
  }

  @Test
  // This test allow to avoid failures due to missing creditor reference id. Although this case will
  // never occur
  // cause transaction-service will refuse transaction activation for WISP_REDIRECT without
  // reference id, it's mandatory
  // test such edge case
  fun `when transaction is performed by WISP_REDIRECT client and creditor reference id is null should get payment notice id`() {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          ClientId.WISP_REDIRECT,
          null))
    val baseTransaction =
      TransactionTestUtils.reduceEvents(*events.toTypedArray())
        as BaseTransactionWithRequestedUserReceipt
    val noticeId =
      WispDeprecation.getPaymentNoticeId(baseTransaction, baseTransaction.paymentNotices.first())
    assertEquals(noticeId, baseTransaction.paymentNotices.first().rptId.noticeId)
  }

  @ParameterizedTest
  @EnumSource(ClientId::class, names = ["WISP_REDIRECT"], mode = EnumSource.Mode.EXCLUDE)
  fun `when transaction is performed by CHECKOUT or IO client should get notice id from rpt`(
    clientId: ClientId
  ) {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          clientId))
    val baseTransaction =
      TransactionTestUtils.reduceEvents(*events.toTypedArray())
        as BaseTransactionWithRequestedUserReceipt
    val noticeId =
      WispDeprecation.getPaymentNoticeId(baseTransaction, baseTransaction.paymentNotices.first())
    assertEquals(noticeId, baseTransaction.paymentNotices.first().rptId.noticeId)
  }

  @ParameterizedTest
  @EnumSource(ClientId::class, names = ["WISP_REDIRECT"], mode = EnumSource.Mode.EXCLUDE)
  fun `when transaction is performed by CHECKOUT or IO client and creditor reference id is null should get notice id from rpt`(
    clientId: ClientId
  ) {
    val events =
      buildTransactionWithUserReceipt(
        TransactionTestUtils.transactionActivateEvent(
          ZonedDateTime.now().toString(),
          EmptyTransactionGatewayActivationData(),
          TransactionTestUtils.USER_ID,
          clientId,
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
