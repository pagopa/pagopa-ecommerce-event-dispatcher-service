package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.storage.queue.models.SendMessageResult
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.documents.v2.Transaction
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentOutcome
import it.pagopa.generated.ecommerce.nodo.v2.dto.CardClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.transactionauthrequests.v1.dto.PaymentInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionStatusDto
import org.junit.jupiter.api.Assertions
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import java.util.*

fun getMockedCardClosePaymentRequest(
    transactionId: TransactionId,
    outcome: ClosePaymentOutcome
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
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            TransactionTestUtils.LOGO_URI,
            TransactionAuthorizationRequestData.CardBrand.VISA,
            TransactionTestUtils.PAYMENT_METHOD_DESCRIPTION
        )

    return CardClosePaymentRequestV2Dto().apply {
        paymentTokens = listOf(UUID.randomUUID().toString())
        this.outcome = CardClosePaymentRequestV2Dto.OutcomeEnum.valueOf(outcome.name)
        idPSP = authEventData.pspId
        paymentMethod = authEventData.paymentTypeCode
        idBrokerPSP = authEventData.brokerName
        idChannel = authEventData.pspChannelCode
        this.transactionId = transactionId.value()
        totalAmount = (authEventData.amount + authEventData.fee).toBigDecimal()
        timestampOperation = OffsetDateTime.now()
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

fun assertEventCodesEquals(
    expectedEvents: List<TransactionEventCode>,
    actual: List<TransactionEvent<*>>
) {
    Assertions.assertIterableEquals(
        expectedEvents, actual.map { TransactionEventCode.valueOf(it.eventCode) })
}

fun assetTransactionStatusEquals(
    expectedStatus: List<it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto>,
    actual: List<Transaction>
) {
    Assertions.assertIterableEquals(expectedStatus, actual.map { it.status })
}
