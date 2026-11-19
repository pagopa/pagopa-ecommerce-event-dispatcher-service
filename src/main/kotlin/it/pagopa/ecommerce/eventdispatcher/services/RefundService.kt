package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.client.NodeForwarderClient
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.client.NpgClient.PaymentMethod
import it.pagopa.ecommerce.commons.documents.v2.ErrorType
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundErrorData
import it.pagopa.ecommerce.commons.domain.v2.TransactionId
import it.pagopa.ecommerce.commons.exceptions.NodeForwarderClientException
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.commons.utils.NpgApiKeyConfiguration
import it.pagopa.ecommerce.commons.utils.RedirectUrlMappingConf
import it.pagopa.ecommerce.commons.utils.bean.redirect.configuration.RedirectUrlMappingCriteria
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundRequestDto as RedirectRefundRequestDto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto as RedirectRefundResponseDto
import java.math.BigDecimal
import java.net.URI
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
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgApiKeyConfiguration: NpgApiKeyConfiguration,
  @Autowired
  private val nodeForwarderRedirectApiClient:
    NodeForwarderClient<RedirectRefundRequestDto, RedirectRefundResponseDto>,
  @Autowired private val redirectBeApiCallUriConf: RedirectUrlMappingConf
) {

  companion object {
    fun exceptionToRefundErrorData(
      transactionStatusDto: TransactionStatusDto,
      throwable: Throwable?
    ): TransactionRefundErrorData? =
      when (throwable) {
        // NPG gateway specific error exception
        is NpgResponseException -> {
          val responseBody =
            (throwable.cause as? WebClientResponseException)?.responseBodyAsString ?: "-"
          val errorDescription =
            if (throwable.errors.isNotEmpty()) {
              "NPG error codes: ${throwable.errors}"
            } else {
              "No NPG error code detected in response body: $responseBody"
            }
          TransactionRefundErrorData(
            transactionStatusDto,
            throwable.statusCode.orElse(null),
            errorDescription,
            ErrorType.KO_RESPONSE_RECEIVED)
        }
        // generic http error response exception
        is WebClientResponseException -> {
          TransactionRefundErrorData(
            transactionStatusDto,
            HttpStatus.valueOf(throwable.statusCode.value()),
            "Gateway response body: [${throwable.responseBodyAsString}]",
            ErrorType.KO_RESPONSE_RECEIVED)
        }
        else -> {
          // try to resolve to a specific exception in the exception chain (see above)
          if (throwable?.cause != null) {
            exceptionToRefundErrorData(
              transactionStatusDto = transactionStatusDto, throwable = throwable.cause)
          } else {
            // otherwise fallback to generic communication error if no specific exception is found
            TransactionRefundErrorData(
              transactionStatusDto,
              null,
              "Error performing refund operation: ${throwable?.message}",
              ErrorType.COMMUNICATION_ERROR)
          }
        }
      }
  }

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
        npgClient
          .refundPayment(
            UUID.fromString(correlationId),
            operationId,
            idempotenceKey,
            amount,
            apiKey,
            "Refund request for transactionId $idempotenceKey and operationId $operationId")
          .doOnNext {
            LogTracingUtils.loggerTracingUtils()
              .success()
              .dependency(LogTracingUtils.NPG_DEPENDENCY)
              .details(
                mapOf(
                  "payment_method" to paymentMethod.toString(),
                  "operation_id" to operationId,
                  "amount" to amount.toString(),
                  "psp_id" to pspId,
                  "correlation_id" to correlationId,
                  "idempotence_key" to idempotenceKey.toString()))
              .attributes(mapOf(LogTracingUtils.AttributeKeys.CORRELATION_ID to correlationId))
              .logInfo(logger, "Requested refund")
          }
          .onErrorMap(NpgResponseException::class.java) { exception: NpgResponseException ->
            LogTracingUtils.loggerTracingUtils()
              .failure()
              .dependency(LogTracingUtils.NPG_DEPENDENCY)
              .details(
                mapOf(
                  "payment_method" to paymentMethod.toString(),
                  "operation_id" to operationId,
                  "amount" to amount.toString(),
                  "psp_id" to pspId,
                  "correlation_id" to correlationId,
                  "idempotence_key" to idempotenceKey.toString()))
              .attributes(mapOf(LogTracingUtils.AttributeKeys.CORRELATION_ID to correlationId))
              .logError(logger, exception, "Exception performing NPG refund")
            val responseStatusCode = exception.statusCode
            responseStatusCode
              .map {
                val errorCodeReason = "Received HTTP error code from NPG: $it"
                if (it.is5xxServerError) {
                  BadGatewayException(errorCodeReason, exception)
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
    pspId: String,
    pspChannelCode: String?
  ): Mono<RedirectRefundResponseDto> =
    redirectBeApiCallUriConf
      .getRedirectUrlForCriteria(
        mapOf(
          RedirectUrlMappingCriteria.TOUCHPOINT to touchpoint,
          RedirectUrlMappingCriteria.PSP_ID to pspId,
          RedirectUrlMappingCriteria.PAYMENT_TYPE_CODE to paymentTypeCode,
          RedirectUrlMappingCriteria.PSP_CHANNEL_ID to pspChannelCode,
        ))
      .fold(
        { Mono.error(it) },
        { urlConfig ->
          nodeForwarderRedirectApiClient
            .proxyRequest(
              RedirectRefundRequestDto()
                .action("refund")
                .idPSPTransaction(pspTransactionId)
                .idTransaction(transactionId.value()),
              // refund path resolved as redirect url with trailing /refunds
              URI.create("${urlConfig.url()}/refunds"),
              transactionId.value(),
              RedirectRefundResponseDto::class.java)
            .doOnNext {
              LogTracingUtils.loggerTracingUtils()
                .success()
                .details(
                  mapOf(
                    "psp_id" to pspId,
                    "psp_transaction_id" to pspTransactionId,
                    "payment_type_code" to paymentTypeCode))
                .logInfo(logger, "Requested Redirect refund")
            }
            .onErrorMap(NodeForwarderClientException::class.java) { exception ->
              val errorCause = exception.cause
              val (httpErrorCode, responseBody) =
                Optional.ofNullable(errorCause)
                  .map {
                    if (it is WebClientResponseException) {
                      Pair(Optional.of(it.statusCode), Optional.ofNullable(it.responseBodyAsString))
                    } else {
                      Pair(Optional.empty(), Optional.empty())
                    }
                  }
                  .orElse(Pair(Optional.empty(), Optional.empty()))

              LogTracingUtils.loggerTracingUtils()
                .failure()
                .details(
                  mapOf(
                    "psp_id" to pspId,
                    "psp_transaction_id" to pspTransactionId,
                    "payment_type_code" to paymentTypeCode,
                    "http_error_code" to httpErrorCode.map { it.toString() }.orElse("N/A"),
                    "http_error_body" to responseBody.orElse("N/A")))
                .logError(
                  logger, exception, "Error performing Redirect refund operation for transaction")
              httpErrorCode
                .map {
                  val errorCodeReason =
                    "Error performing refund for Redirect transaction with id: [${transactionId.value()}] and payment type code: [$paymentTypeCode], HTTP error code: [$it]"
                  if (it.is5xxServerError) {
                    BadGatewayException(errorCodeReason, exception)
                  } else {
                    RefundNotAllowedException(transactionId, errorCodeReason, exception)
                  }
                }
                .orElse(
                  BadGatewayException(
                    "Error performing refund for Redirect transaction with id: [${transactionId.value()}] and payment type code: [$paymentTypeCode]",
                    exception))
            }
            .map { it.body }
        })
}
