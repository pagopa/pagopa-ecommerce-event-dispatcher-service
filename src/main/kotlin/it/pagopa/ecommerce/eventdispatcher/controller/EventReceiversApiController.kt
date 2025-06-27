package it.pagopa.ecommerce.eventdispatcher.controller

import it.pagopa.ecommerce.eventdispatcher.services.EventReceiverService
import it.pagopa.generated.eventdispatcher.server.api.EventReceiversApi
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import it.pagopa.generated.eventdispatcher.server.model.EventReceiverCommandRequestDto
import it.pagopa.generated.eventdispatcher.server.model.EventReceiverStatusResponseDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/** Event receivers commands api controller implementation */
@RestController
@RequestMapping("/event-dispatcher")
class EventReceiversApiController(
  @Autowired private val eventReceiverService: EventReceiverService,
) : EventReceiversApi {

  /** Handle new receiver command */
  override suspend fun newReceiverCommand(
    eventReceiverCommandRequestDto: EventReceiverCommandRequestDto
  ): ResponseEntity<Unit> {
    return eventReceiverService.handleCommand(eventReceiverCommandRequestDto).let {
      ResponseEntity.accepted().build()
    }
  }

  /** Returns receiver statuses */
  override suspend fun retrieveReceiverStatus(
    version: DeploymentVersionDto?
  ): ResponseEntity<EventReceiverStatusResponseDto> {
    return eventReceiverService.getReceiversStatus(deploymentVersionDto = version).let {
      ResponseEntity.ok(it)
    }
  }
}
