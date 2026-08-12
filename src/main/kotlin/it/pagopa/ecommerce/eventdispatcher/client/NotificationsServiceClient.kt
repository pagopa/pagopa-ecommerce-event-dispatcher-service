package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
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
  companion object {
    const val DEPENDENCY = "notifications-service"
  }

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
                LogTracingUtils.loggerTracingUtils()
                  .success()
                  .dependency(DEPENDENCY)
                  .logInfo(logger, "Mail sent successfully")
                response.bodyToMono(NotificationEmailResponseDto::class.java)
              }
              HttpStatus.ACCEPTED -> {
                LogTracingUtils.loggerTracingUtils()
                  .success()
                  .dependency(DEPENDENCY)
                  .logInfo(
                    logger,
                    "Mail sending accepted, retries will be attempted by notifications-service module")
                response.toBodilessEntity().flatMap {
                  Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" })
                }
              }
              else -> response.createException().flatMap { error -> Mono.error(error) }
            }
          }
      }
      .doOnError(WebClientResponseException::class.java) { e: WebClientResponseException ->
        LogTracingUtils.loggerTracingUtils()
          .failure()
          .dependency(DEPENDENCY)
          .details(
            mapOf(
              "http_status" to e.statusCode.toString(), "response_body" to e.responseBodyAsString))
          .logError(logger, e, "Error sending email. Got bad response from notifications-service")
      }
      .doOnError { e: Throwable ->
        LogTracingUtils.loggerTracingUtils()
          .failure()
          .dependency(DEPENDENCY)
          .logErrorWithStackTrace(logger, e, "Error sending email. Got unexpected error")
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
