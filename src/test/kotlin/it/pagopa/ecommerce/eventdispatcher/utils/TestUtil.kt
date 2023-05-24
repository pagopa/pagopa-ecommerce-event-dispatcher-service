package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.storage.queue.models.SendMessageResult
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import java.time.OffsetDateTime
import java.util.*
import reactor.core.publisher.Mono

fun getMockedClosePaymentRequest(
  transactionId: UUID,
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
      "authorizationRequestId",
      TransactionAuthorizationRequestData.PaymentGateway.VPOS,
      TransactionTestUtils.LOGO_URI)

  return ClosePaymentRequestV2Dto().apply {
    paymentTokens = listOf(UUID.randomUUID().toString())
    this.outcome = outcome
    idPSP = authEventData.pspId
    paymentMethod = authEventData.paymentTypeCode
    idBrokerPSP = authEventData.brokerName
    idChannel = authEventData.pspChannelCode
    this.transactionId = transactionId.toString()
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
