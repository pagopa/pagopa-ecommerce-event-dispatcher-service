package it.pagopa.ecommerce.scheduler.utils

import it.pagopa.generated.ecommerce.gateway.v1.dto.PostePayRefundResponseDto
import it.pagopa.generated.ecommerce.nodo.v1.dto.ClosePaymentRequestV2Dto
import it.pagopa.transactions.documents.TransactionAuthorizationRequestData
import java.time.OffsetDateTime
import java.util.*


fun getMockedClosePaymentRequest(
    transactionId: UUID,
    outcome: ClosePaymentRequestV2Dto.OutcomeEnum): ClosePaymentRequestV2Dto {

    val authEventData = TransactionAuthorizationRequestData(
        100,
        1,
        "paymentInstrumentId",
        "pspId",
        "paymentTypeCode",
        "brokerName",
        "pspChannelCode",
        "requestId"
    )

    return ClosePaymentRequestV2Dto().apply {
        paymentTokens = listOf(UUID.randomUUID().toString())
        this.outcome = outcome
        identificativoPsp = authEventData.pspId
        tipoVersamento = authEventData.paymentTypeCode
        identificativoIntermediario = authEventData.brokerName
        identificativoCanale = authEventData.pspChannelCode
        this.transactionId = transactionId.toString()
        totalAmount = (authEventData.amount + authEventData.fee).toBigDecimal()
        timestampOperation = OffsetDateTime.now()
    }
}

fun getMockedRefundRequest(paymentId: String?, result: String = "success"): PostePayRefundResponseDto {
    if (result == "success") {
        return PostePayRefundResponseDto()
            .requestId(UUID.randomUUID().toString())
            .refundOutcome(result)
            .error("")
            .paymentId(paymentId ?: UUID.randomUUID().toString())
    } else {
        return PostePayRefundResponseDto()
            .requestId(UUID.randomUUID().toString())
            .refundOutcome(result)
            .error(result)
            .paymentId(paymentId ?: UUID.randomUUID().toString())
    }
}
