package it.pagopa.ecommerce.eventdispatcher.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.generated.npg.v1.ApiClient as NpgApiClient
import it.pagopa.ecommerce.commons.generated.npg.v1.api.PaymentServicesApi
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadGatewayException
import it.pagopa.ecommerce.eventdispatcher.exceptions.GatewayTimeoutException
import it.pagopa.ecommerce.eventdispatcher.exceptions.RefundNotAllowedException
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.generated.ecommerce.gateway.v1.api.VposInternalApi
import it.pagopa.generated.ecommerce.gateway.v1.api.XPayInternalApi
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import java.util.*
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpStatus
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.util.DefaultUriBuilderFactory
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient

@Component
class PaymentGatewayClient {
  @Autowired @Qualifier("VposApiWebClient") private lateinit var vposApi: VposInternalApi

  @Autowired @Qualifier("XpayApiWebClient") private lateinit var xpayApi: XPayInternalApi

  private val logger = LoggerFactory.getLogger(PaymentGatewayClient::class.java)

  @Bean(name = ["NpgApiWebClient"])
  fun npgApiWebClient(
    @Value("\${npg.uri}") npgClientUrl: String,
    @Value("\${npg.readTimeout}") npgWebClientReadTimeout: Int,
    @Value("\${npg.connectionTimeout}") npgWebClientConnectionTimeout: Int
  ): PaymentServicesApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, npgWebClientConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(npgWebClientReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val defaultUriBuilderFactory = DefaultUriBuilderFactory()
    defaultUriBuilderFactory.encodingMode = DefaultUriBuilderFactory.EncodingMode.NONE

    val webClient =
      NpgApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .uriBuilderFactory(defaultUriBuilderFactory)
        .baseUrl(npgClientUrl)
        .build()

    return PaymentServicesApi(NpgApiClient(webClient).setBasePath(npgClientUrl))
  }

  @Bean
  fun npgClient(
    paymentServicesApi: PaymentServicesApi,
    tracer: Tracer,
    objectMapper: ObjectMapper
  ): NpgClient {
    return NpgClient(paymentServicesApi, tracer, objectMapper)
  }

  fun requestXPayRefund(requestId: UUID): Mono<XPayRefundResponse200Dto> {
    logger.info("Performing XPAY refund for authorization id: [$requestId]")
    return xpayApi.refundXpayRequest(requestId).onErrorMap(
      WebClientResponseException::class.java) { exception: WebClientResponseException ->
      when (exception.statusCode) {
        HttpStatus.NOT_FOUND -> TransactionNotFound(requestId)
        HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
        HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
        HttpStatus.CONFLICT -> RefundNotAllowedException(requestId)
        else -> exception
      }
    }
  }

  fun requestVPosRefund(requestId: UUID): Mono<VposDeleteResponseDto> {
    logger.info("Performing VPOS refund for authorization id: [$requestId]")
    return vposApi.requestPaymentsVposRequestIdDelete(requestId.toString()).onErrorMap(
      WebClientResponseException::class.java) { exception: WebClientResponseException ->
      when (exception.statusCode) {
        HttpStatus.NOT_FOUND -> TransactionNotFound(requestId)
        HttpStatus.GATEWAY_TIMEOUT -> GatewayTimeoutException()
        HttpStatus.INTERNAL_SERVER_ERROR -> BadGatewayException("")
        HttpStatus.CONFLICT -> RefundNotAllowedException(requestId)
        else -> exception
      }
    }
  }
}
