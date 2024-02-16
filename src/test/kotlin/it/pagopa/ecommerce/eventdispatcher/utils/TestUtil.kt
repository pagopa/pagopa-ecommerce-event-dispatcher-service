package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.storage.queue.models.SendMessageResult
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.transactionauthrequests.v1.dto.PaymentInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionStatusDto
import java.time.OffsetDateTime
import java.util.*
import reactor.core.publisher.Mono

fun getMockedClosePaymentRequest(
  transactionId: TransactionId,
  outcome: ClosePaymentRequestV2Dto.OutcomeEnum
): ClosePaymentRequestV2Dto {

  val authEventData =
    TransactionAuthorizationRequestData(
      100,
      1,
      "paymentInstrumentId",
      "pspId",
      "paymentTypeCode",
      "brokerName",
      "pspChannelCode",
      "requestId",
      "pspBusinessName",
      false,
      "authorizationRequestId",
      TransactionAuthorizationRequestData.PaymentGateway.VPOS,
      TransactionTestUtils.LOGO_URI,
      TransactionAuthorizationRequestData.CardBrand.VISA,
      TransactionTestUtils.PAYMENT_METHOD_DESCRIPTION)

  return ClosePaymentRequestV2Dto().apply {
    paymentTokens = listOf(UUID.randomUUID().toString())
    this.outcome = outcome
    idPSP = authEventData.pspId
    paymentMethod = authEventData.paymentTypeCode
    idBrokerPSP = authEventData.brokerName
    idChannel = authEventData.pspChannelCode
    this.transactionId = transactionId.value()
    totalAmount = (authEventData.amount + authEventData.fee).toBigDecimal()
    timestampOperation = OffsetDateTime.now()
  }
}

fun getMockedXPayRefundRequest(
  paymentId: String?,
  result: String = "success",
): XPayRefundResponse200Dto {
  if (result == "success") {
    return XPayRefundResponse200Dto()
      .requestId(UUID.randomUUID().toString())
      .status(XPayRefundResponse200Dto.StatusEnum.CANCELLED)
      .error("")
  } else {
    return XPayRefundResponse200Dto()
      .requestId(UUID.randomUUID().toString())
      .status(XPayRefundResponse200Dto.StatusEnum.CREATED)
      .error("err")
  }
}

fun getMockedTransactionInfoDto(transactionId: UUID): TransactionInfoDto {
  return TransactionInfoDto()
    .transactionId(transactionId.toString())
    .status(TransactionStatusDto.AUTHORIZATION_COMPLETED)
    .authToken("authToken")
    .clientId(TransactionInfoDto.ClientIdEnum.CHECKOUT)
    .feeTotal(300)
    .addPaymentsItem(PaymentInfoDto().amount(3000).paymentToken("paymentToken"))
}

fun getMockedVPosRefundRequest(
  paymentId: String?,
  result: String = "success",
): VposDeleteResponseDto {
  if (result == "success") {
    return VposDeleteResponseDto()
      .requestId(UUID.randomUUID().toString())
      .status(VposDeleteResponseDto.StatusEnum.CANCELLED)
      .error("")
  } else {
    return VposDeleteResponseDto()
      .requestId(UUID.randomUUID().toString())
      .status(VposDeleteResponseDto.StatusEnum.CREATED)
      .error("err")
  }
}

fun queueSuccessfulResponse(): Mono<Response<SendMessageResult>> {
  val sendMessageResult = SendMessageResult()
  sendMessageResult.messageId = "msgId"
  sendMessageResult.timeNextVisible = OffsetDateTime.now()
  return Mono.just(ResponseBase(null, 200, null, sendMessageResult, null))
}

fun getMockedNpgRefundResponse(operationId: String?): RefundResponseDto {
  return RefundResponseDto().operationId(operationId).operationTime("TestTime")
}

const val TRANSIENT_QUEUE_TTL_SECONDS = 30

const val DEAD_LETTER_QUEUE_TTL_SECONDS = -1
