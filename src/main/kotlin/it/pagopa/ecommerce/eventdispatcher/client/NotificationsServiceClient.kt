package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.ecommerce.eventdispatcher.queues.TransactionClosePaymentQueueConsumer
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

  val logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

  fun sendNotificationEmail(
    notificationEmailRequestDto: NotificationEmailRequestDto
  ): Mono<NotificationEmailResponseDto> {
    val response: Mono<NotificationEmailResponseDto> =
      try {
        notificationsServiceApi.sendNotificationEmail(
          notificationsServiceApiKey, notificationEmailRequestDto)
      } catch (e: Exception) {
        Mono.error(e)
      }
    return response
      .doOnError(WebClientResponseException::class.java) { e: WebClientResponseException ->
        logger.error(
          "Got bad response from notifications-service [HTTP {}]: {}",
          e.statusCode,
          e.responseBodyAsString)
      }
      .doOnError { e: Throwable -> logger.error(e.toString()) }
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
