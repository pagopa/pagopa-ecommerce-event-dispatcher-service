package it.pagopa.ecommerce.eventdispatcher.client

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.v1.ApiClient
import it.pagopa.generated.notifications.v1.api.DefaultApi
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import it.pagopa.generated.notifications.v1.dto.NotificationEmailResponseDto
import it.pagopa.generated.notifications.v1.dto.ProblemJsonDto
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.test.StepVerifier
import java.net.URI
import java.time.ZonedDateTime
import java.util.*

class NotificationsServiceClientTest {

    private val defaultApi: DefaultApi = mock()

    private val apiClient: ApiClient = mock()

    private val webClient: WebClient = mock()

    private val bodyUriSpec: WebClient.RequestBodyUriSpec = mock()

    private val headersSpec: WebClient.RequestHeadersSpec<*> = mock()

    private val apiKey = "apiKey"

    private val client = NotificationsServiceClient(defaultApi, apiKey)

    companion object {

        lateinit var mockWebServer: MockWebServer

        val objectMapper = ObjectMapper()

        @JvmStatic
        @BeforeAll
        fun setup() {
            mockWebServer = MockWebServer()
            mockWebServer.start(8080)
            println("Mock web server started on ${mockWebServer.hostName}:${mockWebServer.port}")
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            mockWebServer.shutdown()
            println("Mock web stopped")
        }

    }

    @Test
    fun `should return email outcome`() {
        val notificationEmailRequest =
            NotificationEmailRequestDto()
                .language("it-IT")
                .subject("subject")
                .to("foo@example.com")
                .templateId("template-id")
                .parameters(mapOf(Pair("param1", "value1")))
        val notificationEmailResponseDto = NotificationEmailResponseDto().apply { outcome = "OK" }
        val baseUrl = "http://${mockWebServer.hostName}:${mockWebServer.port}"
        given(apiClient.basePath).willReturn(baseUrl)
        given(defaultApi.apiClient).willReturn(apiClient)
        given(apiClient.webClient).willReturn(WebClient.create(baseUrl))
        mockWebServer.enqueue(
            MockResponse()
                .setBody(objectMapper.writeValueAsString(notificationEmailResponseDto))
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        )
        StepVerifier.create(client.sendNotificationEmail(notificationEmailRequest))
            .expectNext(notificationEmailResponseDto)
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
                        PaymentMethodTemplate("paymentInstrumentId", "paymentMethodLogo", null, false)
                    ),
                    UserTemplate(DataTemplate(null, null, null), "foo@example.com"),
                    CartTemplate(
                        listOf(
                            ItemTemplate(
                                RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, "rptId"),
                                DebtorTemplate(null, null),
                                PayeeTemplate(null, null),
                                "description",
                                "€ 0.00"
                            )
                        ),
                        "€ 0.00"
                    )
                )
            )
        val request =
            NotificationEmailRequestDto()
                .language(successTemplateRequest.language)
                .subject(successTemplateRequest.subject)
                .to(successTemplateRequest.to)
                .templateId(NotificationsServiceClient.SuccessTemplateRequest.TEMPLATE_ID)
                .parameters(successTemplateRequest.templateParameters)
        val notificationEmailResponseDto = NotificationEmailResponseDto().apply { outcome = "OK" }
        val baseUrl = "http://${mockWebServer.hostName}:${mockWebServer.port}"
        given(apiClient.basePath).willReturn(baseUrl)
        given(defaultApi.apiClient).willReturn(apiClient)
        given(apiClient.webClient).willReturn(WebClient.create(baseUrl))
        mockWebServer.enqueue(
            MockResponse()
                .setBody(objectMapper.writeValueAsString(notificationEmailResponseDto))
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        )
        StepVerifier.create(client.sendNotificationEmail(request))
            .expectNext(notificationEmailResponseDto)
            .verifyComplete()
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
                        "€ 12,00"
                    )
                )
            )
        val request =
            NotificationEmailRequestDto()
                .language(koTemplateRequest.language)
                .subject(koTemplateRequest.subject)
                .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
                .parameters(koTemplateRequest.templateParameters)
        val notificationEmailResponseDto = NotificationEmailResponseDto().apply { outcome = "OK" }
        val baseUrl = "http://${mockWebServer.hostName}:${mockWebServer.port}"
        given(apiClient.basePath).willReturn(baseUrl)
        given(defaultApi.apiClient).willReturn(apiClient)
        given(apiClient.webClient).willReturn(WebClient.create(baseUrl))
        mockWebServer.enqueue(
            MockResponse()
                .setBody(objectMapper.writeValueAsString(notificationEmailResponseDto))
                .setResponseCode(200)
                .addHeader("Content-Type", "application/json")
        )
        StepVerifier.create(client.sendNotificationEmail(request))
            .expectNext(notificationEmailResponseDto)
            .verifyComplete()
    }

    @Test
    fun `should return Mono error for KO response received from Notifications service`() {
        val koTemplateRequest =
            NotificationsServiceClient.KoTemplateRequest(
                "foo@example.com",
                "Ops! Il pagamento di € 12,00 tramite PagoPA non è riuscito",
                "it-IT",
                KoTemplate(
                    it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
                        UUID.randomUUID().toString().uppercase(Locale.getDefault()),
                        ZonedDateTime.now().toString(),
                        "€ 12,00"
                    )
                )
            )
        val request =
            NotificationEmailRequestDto()
                .language(koTemplateRequest.language)
                .subject(koTemplateRequest.subject)
                .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
                .parameters(koTemplateRequest.templateParameters)
        val baseUrl = "http://${mockWebServer.hostName}:${mockWebServer.port}"
        given(apiClient.basePath).willReturn(baseUrl)
        given(defaultApi.apiClient).willReturn(apiClient)
        given(apiClient.webClient).willReturn(WebClient.create(baseUrl))
        mockWebServer.enqueue(
            MockResponse()
                .setBody(objectMapper.writeValueAsString(ProblemJsonDto().apply {
                    type = URI.create("http://localhost")
                    title = "Internal server error"
                    status = 500
                    detail = "Error details"
                    instance = URI.create("http://instanceURI")

                }))
                .setResponseCode(500)
                .addHeader("Content-Type", "text/html")
        )
        StepVerifier.create(client.sendNotificationEmail(request))
            .expectError(WebClientResponseException::class.java)
            .verify()
    }

    @Test
    fun `should handle Notifications service HTTP 202 accepted response as OK`() {
        val koTemplateRequest =
            NotificationsServiceClient.KoTemplateRequest(
                "foo@example.com",
                "Ops! Il pagamento di € 12,00 tramite PagoPA non è riuscito",
                "it-IT",
                KoTemplate(
                    it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
                        UUID.randomUUID().toString().uppercase(Locale.getDefault()),
                        ZonedDateTime.now().toString(),
                        "€ 12,00"
                    )
                )
            )
        val request =
            NotificationEmailRequestDto()
                .language(koTemplateRequest.language)
                .subject(koTemplateRequest.subject)
                .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
                .parameters(koTemplateRequest.templateParameters)
        val baseUrl = "http://${mockWebServer.hostName}:${mockWebServer.port}"
        val notificationEmailResponseDto = NotificationEmailResponseDto().apply { outcome = "OK" }
        given(apiClient.basePath).willReturn(baseUrl)
        given(defaultApi.apiClient).willReturn(apiClient)
        given(apiClient.webClient).willReturn(WebClient.create(baseUrl))
        mockWebServer.enqueue(
            MockResponse()
                .setBody("Accepted")
                .setResponseCode(202)
                .addHeader("Content-Type", "text/html")
        )
        StepVerifier.create(client.sendNotificationEmail(request))
            .expectNext(notificationEmailResponseDto)
            .verifyComplete()
    }


}
