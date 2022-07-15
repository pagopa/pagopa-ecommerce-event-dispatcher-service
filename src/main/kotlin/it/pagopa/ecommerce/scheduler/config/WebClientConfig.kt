package it.pagopa.ecommerce.scheduler.config

import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import it.pagopa.generated.ecommerce.gateway.v1.ApiClient as GatewayApiClient
import it.pagopa.generated.ecommerce.gateway.v1.api.PaymentTransactionsControllerApi
import it.pagopa.generated.ecommerce.nodo.v1.ApiClient as NodoApiClient
import it.pagopa.generated.ecommerce.nodo.v1.api.NodoApi
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient
import java.util.concurrent.TimeUnit

@Configuration
class WebClientConfig {
    @Bean
    fun nodoApi(
        @Value("\${nodo.uri}") nodoUri: String,
        @Value("\${nodo.readTimeout}") nodoReadTimeout: Long,
        @Value("\${nodo.connectionTimeout}") nodoConnectionTimeout: Int
    ): NodoApi {
        val httpClient: HttpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nodoConnectionTimeout)
            .doOnConnected { connection ->
                connection.addHandlerLast(
                    ReadTimeoutHandler(
                        nodoReadTimeout,
                        TimeUnit.MILLISECONDS
                    )
                )
            }

        val webClient = NodoApiClient.buildWebClientBuilder().clientConnector(
            ReactorClientHttpConnector(httpClient)
        ).baseUrl(nodoUri).build()

        return NodoApi(NodoApiClient(webClient))
    }

    @Bean(name = ["paymentTransactionGatewayWebClient"])
    fun paymentTransactionGateayWebClient(
        @Value("\${paymentTransactionsGateway.uri}") paymentTransactionGatewayUri: String?,
        @Value("\${paymentTransactionsGateway.readTimeout}") paymentTransactionGatewayReadTimeout: Int,
        @Value("\${paymentTransactionsGateway.connectionTimeout}") paymentTransactionGatewayConnectionTimeout: Int
    ): PaymentTransactionsControllerApi? {
        val httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, paymentTransactionGatewayConnectionTimeout)
            .doOnConnected { connection: Connection ->
                connection.addHandlerLast(
                    ReadTimeoutHandler(
                        paymentTransactionGatewayReadTimeout.toLong(),
                        TimeUnit.MILLISECONDS
                    )
                )
            }
        val webClient = GatewayApiClient.buildWebClientBuilder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .baseUrl(paymentTransactionGatewayUri!!)
            .build()
        return PaymentTransactionsControllerApi(GatewayApiClient(webClient).setBasePath(paymentTransactionGatewayUri))
    }
}