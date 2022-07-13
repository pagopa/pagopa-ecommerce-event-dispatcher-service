package it.pagopa.ecommerce.scheduler.config

import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import it.pagopa.generated.ecommerce.scheduler.node.v1.ApiClient
import it.pagopa.generated.ecommerce.scheduler.node.v1.api.NodoApi
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import reactor.netty.http.client.HttpClient
import java.util.concurrent.TimeUnit


@Configuration
class WebClientsConfig {
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

        val webClient = ApiClient.buildWebClientBuilder().clientConnector(
            ReactorClientHttpConnector(httpClient)
        ).baseUrl(nodoUri).build()

        return NodoApi(ApiClient(webClient))
    }
}