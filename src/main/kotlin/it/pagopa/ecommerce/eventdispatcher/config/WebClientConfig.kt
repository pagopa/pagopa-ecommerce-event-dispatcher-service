package it.pagopa.ecommerce.eventdispatcher.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import it.pagopa.ecommerce.commons.client.NodeForwarderClient
import it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers.ClosePaymentRequestMixin
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundRequestDto as RedirectRefundRequestDto
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto as RedirectRefundResponseDto
import it.pagopa.generated.ecommerce.userstats.ApiClient as UserStatsApiClient
import it.pagopa.generated.ecommerce.userstats.api.UserStatsApi
import it.pagopa.generated.notifications.v1.ApiClient
import it.pagopa.generated.notifications.v1.api.DefaultApi
import it.pagopa.generated.transactionauthrequests.v1.ApiClient as TransanctionsApiClient
import it.pagopa.generated.transactionauthrequests.v1.api.TransactionsApi
import java.util.concurrent.TimeUnit
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.codec.StringDecoder
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.http.codec.json.Jackson2JsonDecoder
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.web.reactive.function.client.ExchangeStrategies
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient

@Configuration
class WebClientConfig {

  fun getNodeObjectMapper(): ObjectMapper {
    val mapper = ObjectMapper()
    mapper.registerModule(JavaTimeModule())
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.addMixIn(ClosePaymentRequestV2Dto::class.java, ClosePaymentRequestMixin::class.java)
    return mapper
  }

  @Bean
  @Qualifier("nodoApiClient")
  fun nodoApi(
    @Value("\${nodo.uri}") nodoUri: String,
    @Value("\${nodo.readTimeout}") nodoReadTimeout: Long,
    @Value("\${nodo.connectionTimeout}") nodoConnectionTimeout: Int,
    @Value("\${nodo.nodeforpm.apikey}") nodoPerPmApiKey: String
  ): WebClient {
    val httpClient: HttpClient =
      HttpClient.create()
        .resolver { it.ndots(1) }
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nodoConnectionTimeout)
        .doOnConnected { connection ->
          connection.addHandlerLast(ReadTimeoutHandler(nodoReadTimeout, TimeUnit.MILLISECONDS))
        }

    val exchangeStrategies =
      ExchangeStrategies.builder()
        .codecs { clientCodecConfigurer: ClientCodecConfigurer ->
          val mapper = getNodeObjectMapper()
          clientCodecConfigurer.registerDefaults(false)
          clientCodecConfigurer.customCodecs().register(StringDecoder.allMimeTypes())
          clientCodecConfigurer
            .customCodecs()
            .register(Jackson2JsonDecoder(mapper, MediaType.APPLICATION_JSON))
          clientCodecConfigurer
            .customCodecs()
            .register(Jackson2JsonEncoder(mapper, MediaType.APPLICATION_JSON))
        }
        .build()

    return WebClient.builder()
      .clientConnector(ReactorClientHttpConnector(httpClient))
      .exchangeStrategies(exchangeStrategies)
      .baseUrl(nodoUri)
      .defaultHeader("ocp-apim-subscription-key", nodoPerPmApiKey)
      .build()
  }

  @Bean(name = ["notificationsServiceWebClient"])
  fun notificationsServiceWebClient(
    @Value("\${notificationsService.uri}") notificationsServiceUri: String,
    @Value("\${notificationsService.readTimeout}") notificationsServiceReadTimeout: Int,
    @Value("\${notificationsService.connectionTimeout}") notificationsServiceConnectionTimeout: Int
  ): DefaultApi {
    val httpClient =
      HttpClient.create()
        .resolver { it.ndots(1) }
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

  @Bean(name = ["transactionsServiceWebClient"])
  fun transactionsServiceWebClient(
    @Value("\${transactionsService.uri}") transactionsServiceUri: String,
    @Value("\${transactionsService.readTimeout}") transactionsServiceReadTimeout: Int,
    @Value("\${transactionsService.connectionTimeout}") transactionsServiceConnectionTimeout: Int,
    @Value("\${transactionsService.apiKey}") transactionsServiceApiKey: String
  ): TransactionsApi {
    val httpClient =
      HttpClient.create()
        .resolver { it.ndots(1) }
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, transactionsServiceConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(transactionsServiceReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      ApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(transactionsServiceUri)
        .build()

    val apiClient = TransanctionsApiClient(webClient).setBasePath(transactionsServiceUri)
    apiClient.setApiKey(transactionsServiceApiKey)
    return TransactionsApi(apiClient)
  }

  /**
   * Build node forwarder proxy api client
   *
   * @param apiKey backend api key
   * @param backendUrl backend URL
   * @param readTimeout read timeout
   * @param connectionTimeout connection timeout
   * @return the build Node forwarder proxy api client
   */
  @Bean
  fun nodeForwarderRedirectApiClient(
    @Value("\${node.forwarder.apiKey}") apiKey: String,
    @Value("\${node.forwarder.url}") backendUrl: String,
    @Value("\${node.forwarder.readTimeout}") readTimeout: Int,
    @Value("\${node.forwarder.connectionTimeout}") connectionTimeout: Int
  ): NodeForwarderClient<RedirectRefundRequestDto, RedirectRefundResponseDto> {
    return NodeForwarderClient<RedirectRefundRequestDto, RedirectRefundResponseDto>(
      apiKey, backendUrl, readTimeout, connectionTimeout)
  }

  /**
   * Build user stats proxy api client
   *
   * @param userStatsServiceApiKey backend api key
   * @param userStatsServiceUri backend URL
   * @param userStatsServiceReadTimeout read timeout
   * @param userStatsServiceConnectionTimeout connection timeout
   * @return the user stats proxy api client
   */
  @Bean(name = ["userStatsServiceWebClient"])
  fun userStatsServiceWebClient(
    @Value("\${userStatsService.uri}") userStatsServiceUri: String,
    @Value("\${userStatsService.readTimeout}") userStatsServiceReadTimeout: Int,
    @Value("\${userStatsService.connectionTimeout}") userStatsServiceConnectionTimeout: Int,
    @Value("\${userStatsService.apiKey}") userStatsServiceApiKey: String
  ): UserStatsApi {
    val httpClient =
      HttpClient.create()
        .resolver { it.ndots(1) }
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, userStatsServiceConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(userStatsServiceReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      ApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(userStatsServiceUri)
        .build()

    val apiClient = UserStatsApiClient(webClient).setBasePath(userStatsServiceUri)
    apiClient.setApiKey(userStatsServiceApiKey)
    return UserStatsApi(apiClient)
  }
}
