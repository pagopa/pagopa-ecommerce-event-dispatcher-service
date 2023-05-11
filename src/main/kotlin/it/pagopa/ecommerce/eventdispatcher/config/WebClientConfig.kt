package it.pagopa.ecommerce.eventdispatcher.config

import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import it.pagopa.generated.ecommerce.gateway.v1.api.VposApi
import it.pagopa.generated.ecommerce.gateway.v1.api.XPayApi
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.notifications.v1.ApiClient
import it.pagopa.generated.notifications.v1.api.DefaultApi
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient
import java.util.concurrent.TimeUnit
import it.pagopa.generated.ecommerce.gateway.v1.ApiClient as GatewayApiClient
import it.pagopa.generated.ecommerce.nodo.v2.ApiClient as NodoApiClient

@Configuration
class WebClientConfig {
  @Bean
  fun nodoApi(
    @Value("\${nodo.uri}") nodoUri: String,
    @Value("\${nodo.readTimeout}") nodoReadTimeout: Long,
    @Value("\${nodo.connectionTimeout}") nodoConnectionTimeout: Int
  ): NodoApi {
    val httpClient: HttpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nodoConnectionTimeout)
        .doOnConnected { connection ->
          connection.addHandlerLast(ReadTimeoutHandler(nodoReadTimeout, TimeUnit.MILLISECONDS))
        }

    val webClient =
      NodoApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(nodoUri)
        .build()

    val nodoApiClient = NodoApiClient(webClient).apply { basePath = nodoUri }

    return NodoApi(nodoApiClient)
  }

  @Bean(name = ["VposApiWebClient"])
  fun vposApiWebClient(
    @Value("\${paymentTransactionsGateway.uri}") paymentTransactionGatewayUri: String,
    @Value("\${paymentTransactionsGateway.readTimeout}") paymentTransactionGatewayReadTimeout: Int,
    @Value("\${paymentTransactionsGateway.connectionTimeout}")
    paymentTransactionGatewayConnectionTimeout: Int,
    @Value("\${paymentTransactionsGateway.apiKey}") paymentTransactionGatewayApiKey: String
  ): VposApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, paymentTransactionGatewayConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(
              paymentTransactionGatewayReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      GatewayApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(paymentTransactionGatewayUri)
        .build()

    val gatewayApiClient = GatewayApiClient(webClient)
    gatewayApiClient.setApiKey(paymentTransactionGatewayApiKey)
    gatewayApiClient.basePath = paymentTransactionGatewayUri

    return VposApi(gatewayApiClient)
  }

  @Bean(name = ["XpayApiWebClient"])
  fun xpayApiWebClient(
    @Value("\${paymentTransactionsGateway.uri}") paymentTransactionGatewayUri: String,
    @Value("\${paymentTransactionsGateway.readTimeout}") paymentTransactionGatewayReadTimeout: Int,
    @Value("\${paymentTransactionsGateway.connectionTimeout}")
    paymentTransactionGatewayConnectionTimeout: Int,
    @Value("\${paymentTransactionsGateway.apiKey}") paymentTransactionGatewayApiKey: String
  ): XPayApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, paymentTransactionGatewayConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(
              paymentTransactionGatewayReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      GatewayApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(paymentTransactionGatewayUri)
        .build()

    val gatewayApiClient = GatewayApiClient(webClient)
    gatewayApiClient.setApiKey(paymentTransactionGatewayApiKey)
    gatewayApiClient.basePath = paymentTransactionGatewayUri

    return XPayApi(gatewayApiClient)
  }

  @Bean(name = ["notificationsServiceWebClient"])
  fun notificationsServiceWebClient(
    @Value("\${notificationsService.uri}") notificationsServiceUri: String,
    @Value("\${notificationsService.readTimeout}") notificationsServiceReadTimeout: Int,
    @Value("\${notificationsService.connectionTimeout}") notificationsServiceConnectionTimeout: Int
  ): DefaultApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, notificationsServiceConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(notificationsServiceReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      ApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(notificationsServiceUri)
        .build()
    return DefaultApi(ApiClient(webClient).setBasePath(notificationsServiceUri))
  }
}
