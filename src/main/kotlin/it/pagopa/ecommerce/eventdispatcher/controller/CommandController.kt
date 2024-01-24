package it.pagopa.ecommerce.eventdispatcher.controller

import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherCommandsTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterHandlerService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
class CommandController(
  @Autowired
  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper,
  @Autowired val inboundChannelAdapterHandlerService: InboundChannelAdapterHandlerService
) {

  @GetMapping("channels")
  fun getAllInboundChannels(): Mono<ResponseEntity<ChannelsStatusResponse>> {
    return Flux.fromIterable(inboundChannelAdapterHandlerService.getAllChannelStatus())
      .collectList()
      .map { ResponseEntity.ok().body(ChannelsStatusResponse(it)) }
  }

  @PostMapping("channels/stop")
  fun shutdownAll(): Mono<ResponseEntity<Unit>> {
    eventDispatcherCommandsTemplateWrapper.unwrap().opsForStream<String, String>().trim("test", 0)
    eventDispatcherCommandsTemplateWrapper
      .unwrap()
      .opsForStream<String, EventDispatcherReceiverCommand>()
      .add(
        ObjectRecord.create(
          "test",
          EventDispatcherReceiverCommand(
            receiverCommand = EventDispatcherReceiverCommand.ReceiverCommand.STOP)))
    return Mono.just(inboundChannelAdapterHandlerService.invokeCommandForAllEndpoints("stop")).map {
      ResponseEntity.ok(Unit)
    }
  }

  @PostMapping("channels/start")
  fun startAll(): Mono<ResponseEntity<Unit>> {
    return Mono.just(inboundChannelAdapterHandlerService.invokeCommandForAllEndpoints("start"))
      .map { ResponseEntity.ok(Unit) }
  }

  class ChannelsStatusResponse(
    val services: List<InboundChannelAdapterHandlerService.ChannelStatus>
  )
}
