package it.pagopa.ecommerce.eventdispatcher.controller

import it.pagopa.ecommerce.eventdispatcher.exceptions.NoEventReceiverStatusFound
import it.pagopa.ecommerce.eventdispatcher.services.EventReceiverService
import it.pagopa.ecommerce.eventdispatcher.validation.BeanValidationConfiguration
import it.pagopa.generated.eventdispatcher.server.model.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.given
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.reactive.server.WebTestClient

@OptIn(ExperimentalCoroutinesApi::class)
@WebFluxTest(EventReceiversApiController::class)
@Import(BeanValidationConfiguration::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class EventReceiversApiControllerTest {

  @Autowired lateinit var webClient: WebTestClient

  @MockitoBean lateinit var eventReceiverService: EventReceiverService

  @Test
  fun `Should handle command creation successfully`() = runTest {
    val request = EventReceiverCommandRequestDto(EventReceiverCommandRequestDto.Command.START)
    given(eventReceiverService.handleCommand(request)).willReturn(Unit)
    webClient
      .post()
      .uri("/event-dispatcher/event-receivers/commands")
      .contentType(MediaType.APPLICATION_JSON)
      .bodyValue(request)
      .exchange()
      .expectStatus()
      .isAccepted
  }

  @Test
  fun `Should return 400 bad request for invalid command request`() = runTest {
    val expectedProblemJsonDto =
      ProblemJsonDto(title = "Bad request", status = 400, detail = "Input request is invalid.")
    webClient
      .post()
      .uri("/event-dispatcher/event-receivers/commands")
      .contentType(MediaType.APPLICATION_JSON)
      .bodyValue(
        """
                {
                  "command": "FAKE"
                }
            """.trimIndent())
      .exchange()
      .expectStatus()
      .isBadRequest
      .expectBody(ProblemJsonDto::class.java)
      .isEqualTo(expectedProblemJsonDto)
  }

  @Test
  fun `Should return 500 Internal Server Error for uncaught exception`() = runTest {
    val expectedProblemJsonDto =
      ProblemJsonDto(
        title = "Internal Server Error",
        status = 500,
        detail = "An unexpected error occurred processing the request")
    val request = EventReceiverCommandRequestDto(EventReceiverCommandRequestDto.Command.START)
    given(eventReceiverService.handleCommand(request))
      .willThrow(RuntimeException("Uncaught exception"))
    webClient
      .post()
      .uri("/event-dispatcher/event-receivers/commands")
      .contentType(MediaType.APPLICATION_JSON)
      .bodyValue(request)
      .exchange()
      .expectStatus()
      .isEqualTo(500)
      .expectBody(ProblemJsonDto::class.java)
      .isEqualTo(expectedProblemJsonDto)
  }

  @Test
  fun `Should return receiver statuses successfully`() = runTest {
    val response =
      EventReceiverStatusResponseDto(
        listOf(
          EventReceiverStatusDto(
            instanceId = "instanceId",
            deploymentVersion = DeploymentVersionDto.PROD,
            receiverStatuses =
              listOf(ReceiverStatusDto(status = ReceiverStatusDto.Status.DOWN, name = "name")))))
    given(eventReceiverService.getReceiversStatus(null)).willReturn(response)
    webClient
      .get()
      .uri("/event-dispatcher/event-receivers/status")
      .exchange()
      .expectStatus()
      .isOk
      .expectBody(EventReceiverStatusResponseDto::class.java)
      .isEqualTo(response)
  }

  @Test
  fun `Should return 404 for no receiver information found successfully`() = runTest {
    val expectedProblemJsonDto =
      ProblemJsonDto(
        title = "Not found", status = 404, detail = "No data found for receiver statuses")
    given(eventReceiverService.getReceiversStatus(null)).willThrow(NoEventReceiverStatusFound())
    webClient
      .get()
      .uri("/event-dispatcher/event-receivers/status")
      .exchange()
      .expectStatus()
      .isNotFound
      .expectBody(ProblemJsonDto::class.java)
      .isEqualTo(expectedProblemJsonDto)
  }
}
