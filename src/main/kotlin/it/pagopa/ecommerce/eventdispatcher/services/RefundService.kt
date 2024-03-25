package it.pagopa.ecommerce.eventdispatcher.services

import io.vavr.control.Either
import it.pagopa.ecommerce.commons.client.NodeForwarderClient
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.exceptions.NodeForwarderClientException
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.exceptions.RedirectConfigurationException
import it.pagopa.ecommerce.commons.exceptions.RedirectConfigurationType
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.RefundResponseDto
import it.pagopa.ecommerce.commons.utils.NpgPspApiKeysConfig
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
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
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val npgClient: NpgClient,
  @Autowired private val npgCardsPspApiKey: NpgPspApiKeysConfig,
  @Autowired
  private val nodeForwarderRedirectApiClient:
    NodeForwarderClient<RedirectRefundRequestDto, RedirectRefundResponseDto>,
  @Autowired private val redirectBeApiCallUriMap: Map<String, URI>
) {

  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  fun requestNpgRefund(
    operationId: String,
    idempotenceKey: UUID,
    amount: BigDecimal,
    pspId: String,
    correlationId: String
  ): Mono<RefundResponseDto> {
    return npgCardsPspApiKey[pspId].fold(
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
          .onErrorMap(NpgResponseException::class.java) { exception: NpgResponseException ->
            val responseStatusCode = exception.statusCode
            responseStatusCode
              .map {
                val errorCodeReason = "Received HTTP error code from NPG: $it"
                if (it.is5xxServerError) {
                  BadGatewayException(errorCodeReason)
                } else {
                  RefundNotAllowedException(idempotenceKey, errorCodeReason)
                }
              }
              .orElse(RefundNotAllowedException(idempotenceKey, "Unknown NPG HTTP response code"))
          }
      })
  }

  fun requestVposRefund(requestID: String): Mono<VposDeleteResponseDto> {
    return paymentGatewayClient.requestVPosRefund(UUID.fromString(requestID))
  }

  fun requestXpayRefund(requestID: String): Mono<XPayRefundResponse200Dto> {
    return paymentGatewayClient.requestXPayRefund(UUID.fromString(requestID))
  }

  fun requestRedirectRefund(
    transactionId: TransactionId,
    pspTransactionId: String,
    paymentTypeCode: String,
    pspId: String
  ): Mono<RedirectRefundResponseDto> =
    getRedirectUri(pspId, paymentTypeCode)
      .fold(
        { Mono.error(it) },
        { uri ->
          logger.info(
            "Redirect transaction refund. TransactionId: [{}], pspTransactionId: [{}], payment type code: [{}}",
            transactionId,
            pspTransactionId,
            paymentTypeCode)
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
              httpErrorCode
                .map {
                  val errorCodeReason =
                    "Received HTTP error code performing refund for Redirect transaction made with payment type code: [$paymentTypeCode] -> [$it]"
                  if (it.is5xxServerError) {
                    BadGatewayException(errorCodeReason)
                  } else {
                    RefundNotAllowedException(transactionId.uuid, errorCodeReason)
                  }
                }
                .orElse(
                  BadGatewayException(
                    "Error performing refund for Redirect transaction with id: [${transactionId.value()}] and payment type code: [$paymentTypeCode]"))
            }
            .map { it.body }
        })

  private fun getRedirectUri(
    pspId: String,
    paymentTypeCode: String
  ): Either<RedirectConfigurationException, URI> {
    val urlKey = "$pspId-$paymentTypeCode"
    return Optional.ofNullable(redirectBeApiCallUriMap[urlKey])
      .map { Either.right<RedirectConfigurationException, URI>(it) }
      .orElse(
        Either.left(
          RedirectConfigurationException(
            "Missing key for redirect return url with key: [$urlKey]",
            RedirectConfigurationType.BACKEND_URLS)))
  }
}
