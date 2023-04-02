package it.pagopa.ecommerce.eventdispatcher.client

import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.v1.api.DefaultApi
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
import java.nio.charset.Charset
import java.time.ZonedDateTime
import java.util.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import org.springframework.http.HttpHeaders
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class NotificationsServiceClientTest {

  private val defaultApi: DefaultApi = mock()

  private val apiKey = "apiKey"

  private val client = NotificationsServiceClient(defaultApi, apiKey)

  @Test
  fun `should return email outcome`() {
    val notificationEmailRequest =
      NotificationEmailRequestDto()
        .language("it-IT")
        .subject("subject")
        .to("foo@example.com")
        .templateId("template-id")
        .parameters(mapOf(Pair("param1", "value1")))
    val expected = NotificationEmailResponseDto().outcome("OK")
    Mockito.`when`(defaultApi.sendNotificationEmail(apiKey, notificationEmailRequest))
      .thenReturn(Mono.just(expected))
    StepVerifier.create(client.sendNotificationEmail(notificationEmailRequest))
      .expectNext(expected)
      .verifyComplete()
  }

  @Test
  fun `should return email outcome with success template`() {
    val successTemplateRequest =
      NotificationsServiceClient.SuccessTemplateRequest(
        "foo@example.com",
        "Hai pagato un avviso di pagamento PagoPA",
        "it-IT",
        SuccessTemplate(
          TransactionTemplate(
            "transactionId",
            ZonedDateTime.now().toString(),
            "€ 0.00",
            PspTemplate("pspId", FeeTemplate("€ 0.00")),
            "RRN",
            "authorizationCode",
            PaymentMethodTemplate("paymentInstrumentId", "paymentMethodLogo", null, false)),
          UserTemplate(DataTemplate(null, null, null), "foo@example.com"),
          CartTemplate(
            listOf(
              ItemTemplate(
                RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, "rptId"),
                DebtorTemplate(null, null),
                PayeeTemplate(null, null),
                "description",
                "€ 0.00")),
            "€ 0.00")))
    val request =
      NotificationEmailRequestDto()
        .language(successTemplateRequest.language)
        .subject(successTemplateRequest.subject)
        .to(successTemplateRequest.to)
        .templateId(NotificationsServiceClient.SuccessTemplateRequest.TEMPLATE_ID)
        .parameters(successTemplateRequest.templateParameters)
    val expected = NotificationEmailResponseDto().outcome("OK")
    given(defaultApi.sendNotificationEmail(apiKey, request)).willReturn(Mono.just(expected))
    StepVerifier.create(client.sendNotificationEmail(request)).expectNext(expected).verifyComplete()
  }

  @Test
  fun `should return email outcome with KO template`() {
    val koTemplateRequest =
      NotificationsServiceClient.KoTemplateRequest(
        "foo@example.com",
        "Ops! Il pagamento di € 12,00 tramite PagoPA non è riuscito",
        "it-IT",
        KoTemplate(
          it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
            UUID.randomUUID().toString().uppercase(Locale.getDefault()),
            ZonedDateTime.now().toString(),
            "€ 12,00")))
    val request =
      NotificationEmailRequestDto()
        .language(koTemplateRequest.language)
        .subject(koTemplateRequest.subject)
        .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
        .parameters(koTemplateRequest.templateParameters)
    val expected = NotificationEmailResponseDto().outcome("OK")
    given(defaultApi.sendNotificationEmail(apiKey, request)).willReturn(Mono.just(expected))
    StepVerifier.create(client.sendNotificationEmail(request)).expectNext(expected).verifyComplete()
  }

  @Test
  fun `should return Mono error for exception during notificationService invocation`() {
    val koTemplateRequest =
      NotificationsServiceClient.KoTemplateRequest(
        "foo@example.com",
        "Ops! Il pagamento di € 12,00 tramite PagoPA non è riuscito",
        "it-IT",
        KoTemplate(
          it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
            UUID.randomUUID().toString().uppercase(Locale.getDefault()),
            ZonedDateTime.now().toString(),
            "€ 12,00")))
    val request =
      NotificationEmailRequestDto()
        .language(koTemplateRequest.language)
        .subject(koTemplateRequest.subject)
        .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
        .parameters(koTemplateRequest.templateParameters)

    given(defaultApi.sendNotificationEmail(apiKey, request))
      .willReturn(Mono.error(RuntimeException("Error")))
    StepVerifier.create(client.sendNotificationEmail(request))
      .expectError(RuntimeException::class.java)
      .verify()
  }

  @Test
  fun `should return Mono error for exception thrown during notificationService invocation`() {
    val koTemplateRequest =
      NotificationsServiceClient.KoTemplateRequest(
        "foo@example.com",
        "Ops! Il pagamento di € 12,00 tramite PagoPA non è riuscito",
        "it-IT",
        KoTemplate(
          it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
            UUID.randomUUID().toString().uppercase(Locale.getDefault()),
            ZonedDateTime.now().toString(),
            "€ 12,00")))
    val request =
      NotificationEmailRequestDto()
        .language(koTemplateRequest.language)
        .subject(koTemplateRequest.subject)
        .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
        .parameters(koTemplateRequest.templateParameters)

    given(defaultApi.sendNotificationEmail(apiKey, request))
      .willThrow(
        WebClientResponseException.create(
          404, "Not found", HttpHeaders.EMPTY, ByteArray(0), Charset.defaultCharset()))
    StepVerifier.create(client.sendNotificationEmail(request))
      .expectError(RuntimeException::class.java)
      .verify()
  }
}
