package it.pagopa.ecommerce.eventdispatcher.controller

import it.pagopa.ecommerce.eventdispatcher.services.EventReceiverService
import it.pagopa.generated.eventdispatcher.server.api.EventReceiversApi
import it.pagopa.generated.eventdispatcher.server.model.EventReceiverCommandRequestDto
import it.pagopa.generated.eventdispatcher.server.model.EventReceiverStatusResponseDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController

/** Event receivers commands api controller implementation */
@RestController
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
  override suspend fun retrieveReceiverStatus(): ResponseEntity<EventReceiverStatusResponseDto> {
    return eventReceiverService.getReceiversStatus().let { ResponseEntity.ok(it) }
  }
}
