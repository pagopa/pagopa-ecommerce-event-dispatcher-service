package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NodeForwarderClient
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.client.NpgClient.PaymentMethod
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.exceptions.NodeForwarderClientException
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.utils.NpgApiKeyConfiguration
import it.pagopa.ecommerce.commons.utils.RedirectKeysConfiguration
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundRequestDto as RedirectRefundRequestDto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto as RedirectRefundResponseDto
import java.math.BigDecimal
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class RefundService(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgApiKeyConfiguration: NpgApiKeyConfiguration,
  @Autowired
  private val nodeForwarderRedirectApiClient:
    NodeForwarderClient<RedirectRefundRequestDto, RedirectRefundResponseDto>,
  @Autowired private val redirectBeApiCallUriConf: RedirectKeysConfiguration
) {

  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  fun requestNpgRefund(
    operationId: String,
    idempotenceKey: UUID,
    amount: BigDecimal,
    pspId: String,
    correlationId: String,
    paymentMethod: PaymentMethod
  ): Mono<RefundResponseDto> {
    return npgApiKeyConfiguration[paymentMethod, pspId].fold(
      { ex -> Mono.error(ex) },
      { apiKey ->
        logger.info(
          "Performing NPG refund for transaction with id: [{}] and paymentMethod: [{}]. OperationId: [{}], amount: [{}], pspId: [{}], correlationId: [{}]",
          idempotenceKey,
          paymentMethod,
          operationId,
          amount,
          pspId,
          correlationId)
        npgClient
          .refundPayment(
            UUID.fromString(correlationId),
            operationId,
            idempotenceKey,
            amount,
            apiKey,
            "Refund request for transactionId $idempotenceKey and operationId $operationId")
          .onErrorMap(NpgResponseException::class.java) { exception: NpgResponseException ->
            logger.error(
              "Exception performing NPG refund for transactionId: [$idempotenceKey] and operationId: [$operationId]",
              exception)
            val responseStatusCode = exception.statusCode
            responseStatusCode
              .map {
                val errorCodeReason = "Received HTTP error code from NPG: $it"
                if (it.is5xxServerError) {
                  BadGatewayException(errorCodeReason)
                } else {
                  RefundNotAllowedException(
                    TransactionId(idempotenceKey), errorCodeReason, exception)
                }
              }
              .orElse(exception)
          }
      })
  }

  fun requestRedirectRefund(
    transactionId: TransactionId,
    touchpoint: String,
    pspTransactionId: String,
    paymentTypeCode: String,
    pspId: String
  ): Mono<RedirectRefundResponseDto> =
    redirectBeApiCallUriConf
      .getRedirectUrlForPsp(touchpoint, pspId, paymentTypeCode)
      .fold(
        { Mono.error(it) },
        { uri ->
          logger.info(
            "Processing Redirect transaction refund. TransactionId: [{}], pspTransactionId: [{}], payment type code: [{}], pspId: [{}], touchpoint: [{}]",
            transactionId.value(),
            pspTransactionId,
            paymentTypeCode,
            pspId,
            touchpoint)
          nodeForwarderRedirectApiClient
            .proxyRequest(
              RedirectRefundRequestDto()
                .action("refund")
                .idPSPTransaction(pspTransactionId)
                .idTransaction(transactionId.value()),
              uri,
              transactionId.value(),
              RedirectRefundResponseDto::class.java)
            .onErrorMap(NodeForwarderClientException::class.java) { exception ->
              val errorCause = exception.cause
              val httpErrorCode: Optional<HttpStatus> =
                Optional.ofNullable(errorCause).map {
                  if (it is WebClientResponseException) {
                    it.statusCode
                  } else {
                    null
                  }
                }
              logger.error(
                "Error performing Redirect refund operation for transaction with id: [${transactionId.value()}]. psp id: [$pspId], pspTransactionId: [$pspTransactionId], paymentTypeCode: [$paymentTypeCode], received HTTP response error code: [${
                                    httpErrorCode.map { it.toString() }.orElse("N/A")
                                }]",
                exception)
              httpErrorCode
                .map {
                  val errorCodeReason =
                    "Error performing refund for Redirect transaction with id: [${transactionId.value()}] and payment type code: [$paymentTypeCode], HTTP error code: [$it]"
                  if (it.is5xxServerError) {
                    BadGatewayException(errorCodeReason)
                  } else {
                    RefundNotAllowedException(transactionId, errorCodeReason, exception)
                  }
                }
                .orElse(
                  BadGatewayException(
                    "Error performing refund for Redirect transaction with id: [${transactionId.value()}] and payment type code: [$paymentTypeCode]"))
            }
            .map { it.body }
        })
}
