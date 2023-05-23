package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.SuccessTemplate
import it.pagopa.generated.notifications.v1.api.DefaultApi
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
import kotlinx.coroutines.reactor.mono
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
    val transactionId = getTransactionIdFromParameters(notificationEmailRequestDto.parameters)
    return notificationsServiceApi.apiClient.webClient
      .post()
      .uri("${notificationsServiceApi.apiClient.basePath}/emails")
      .header("ocp-apim-subscription-key", notificationsServiceApiKey)
      .body(mono { notificationEmailRequestDto }, NotificationEmailRequestDto::class.java)
      .exchangeToMono { response ->
        logger.info(
          "Notification for transaction with id $transactionId sent, notifications-service code ${response.statusCode()}")

        when (response.statusCode()) {
          HttpStatus.OK -> {
            logger.info("Mail sent successfully for transaction id: $transactionId")
            response.bodyToMono(NotificationEmailResponseDto::class.java)
          }
          HttpStatus.ACCEPTED -> {
            logger.info(
              "Mail sending accepted for transaction id: $transactionId, retries will be attempted by notifications-service module")
            response.toBodilessEntity().flatMap {
              Mono.just(NotificationEmailResponseDto().apply { outcome = "OK" })
            }
          }
          else -> response.createException().flatMap { error -> Mono.error(error) }
        }
      }
      .doOnError(WebClientResponseException::class.java) { e: WebClientResponseException ->
        logger.error(
          "Error sending email for transaction id: {}. Got bad response from notifications-service [HTTP {}]: {}",
          transactionId,
          e.statusCode,
          e.responseBodyAsString)
      }
      .doOnError { e: Throwable -> logger.error(e.toString()) }
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
