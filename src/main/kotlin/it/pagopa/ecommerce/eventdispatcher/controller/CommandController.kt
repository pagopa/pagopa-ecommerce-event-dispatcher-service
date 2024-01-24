package it.pagopa.ecommerce.eventdispatcher.controller

import it.pagopa.ecommerce.eventdispatcher.services.EventReceiversService
import it.pagopa.generated.eventdispatcher.server.api.EventReceiversApi
import it.pagopa.generated.eventdispatcher.server.model.EventReceiverCommandRequestDto
import it.pagopa.generated.eventdispatcher.server.model.EventReceiverStatusResponseDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController

@RestController
class EventReceiversApiController(
  @Autowired private val eventReceiversService: EventReceiversService,
) : EventReceiversApi {

  override suspend fun newReceiverCommand(
    eventReceiverCommandRequestDto: EventReceiverCommandRequestDto
  ): ResponseEntity<Unit> {
    return eventReceiversService.handleCommand(eventReceiverCommandRequestDto).let {
      ResponseEntity.accepted().build()
    }
  }

  override suspend fun retrieveReceiverStatus(): ResponseEntity<EventReceiverStatusResponseDto> {
    return eventReceiversService.getReceiversStatus().let { ResponseEntity.ok(it) }
  }
}
