package it.pagopa.ecommerce.eventdispatcher.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.generated.npg.v1.ApiClient as NpgApiClient
import it.pagopa.ecommerce.commons.generated.npg.v1.api.PaymentServicesApi
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.util.DefaultUriBuilderFactory
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient

@Component
class PaymentGatewayClient {

  private val logger = LoggerFactory.getLogger(PaymentGatewayClient::class.java)

  @Bean(name = ["NpgApiWebClient"])
  fun npgApiWebClient(
    @Value("\${npg.uri}") npgClientUrl: String,
    @Value("\${npg.readTimeout}") npgWebClientReadTimeout: Int,
    @Value("\${npg.connectionTimeout}") npgWebClientConnectionTimeout: Int
  ): PaymentServicesApi {
    val httpClient =
      HttpClient.create()
        .resolver { it.ndots(1) }
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
}
