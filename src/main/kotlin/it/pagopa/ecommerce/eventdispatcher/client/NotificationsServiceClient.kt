package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.mdcutilities.EventDispatcherTracingUtils
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.SuccessTemplate
import it.pagopa.generated.notifications.v1.api.DefaultApi
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono

@Component
class NotificationsServiceClient(
  @Autowired
  @Qualifier("notificationsServiceWebClient")
  private val notificationsServiceApi: DefaultApi,
  @Value("\${notificationsService.apiKey}") private val notificationsServiceApiKey: String
) {

  val logger: Logger = LoggerFactory.getLogger(NotificationsServiceClient::class.java)

  fun sendNotificationEmail(
    notificationEmailRequestDto: NotificationEmailRequestDto
  ): Mono<NotificationEmailResponseDto> {
    return Mono.defer {
        notificationsServiceApi.apiClient.webClient
          .post()
          .uri("${notificationsServiceApi.apiClient.basePath}/emails")
          .header("ocp-apim-subscription-key", notificationsServiceApiKey)
          .bodyValue(notificationEmailRequestDto)
          .exchangeToMono { response ->
            when (response.statusCode()) {
              HttpStatus.OK -> {
                EventDispatcherTracingUtils.withContextDetailsMdc(
                  null,
                  mapOf(EventDispatcherTracingUtils.TracingEntry.EVENT_OUTCOME.key to "success")) {
                  logger.info("Mail sent successfully")
                }
                response.bodyToMono(NotificationEmailResponseDto::class.java)
              }
              HttpStatus.ACCEPTED -> {
                EventDispatcherTracingUtils.withContextDetailsMdc(
                  null,
                  mapOf(EventDispatcherTracingUtils.TracingEntry.EVENT_OUTCOME.key to "success")) {
                  logger.info(
                    "Mail sending accepted, retries will be attempted by notifications-service module")
                }
                response.toBodilessEntity().flatMap {
                  Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" })
                }
              }
              else -> response.createException().flatMap { error -> Mono.error(error) }
            }
          }
      }
      .doOnError(WebClientResponseException::class.java) { e: WebClientResponseException ->
        EventDispatcherTracingUtils.withContextDetailsMdc(
          mapOf("http_status" to e.statusCode, "response_body" to e.responseBodyAsString),
          mapOf(EventDispatcherTracingUtils.TracingEntry.EVENT_OUTCOME.key to "failure")) {
          logger.error("Error sending email. Got bad response from notifications-service")
        }
      }
      .doOnError { e: Throwable ->
        EventDispatcherTracingUtils.withErrorMdc(
          e, mapOf(EventDispatcherTracingUtils.TracingEntry.EVENT_OUTCOME.key to "failure")) {
          logger.error("Error sending email", e)
        }
      }
  }

  fun getTransactionIdFromParameters(parameters: Any): String? =
    when (parameters) {
      is KoTemplate -> parameters.transaction.id
      is SuccessTemplate -> parameters.transaction.id
      else -> "N/A"
    }

  data class SuccessTemplateRequest(
    val to: String,
    val subject: String,
    val language: String,
    val templateParameters: SuccessTemplate
  ) {
    companion object {
      const val TEMPLATE_ID = "success"
    }
  }

  data class KoTemplateRequest(
    val to: String,
    val subject: String,
    val language: String,
    val templateParameters: KoTemplate
  ) {
    companion object {
      const val TEMPLATE_ID = "ko"
    }
  }
}
