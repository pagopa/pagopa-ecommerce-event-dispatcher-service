package it.pagopa.ecommerce.eventdispatcher.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.ApplicationContext
import org.springframework.http.ResponseEntity
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.QueueChannel
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.GenericMessage
import org.springframework.messaging.support.MessageBuilder
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
class CommandController(
  @Autowired private val applicationContext: ApplicationContext,
  @Autowired @Qualifier("controlBusInCH") private val controlBusInput: DirectChannel,
  @Autowired @Qualifier("controlBusOutCH") private val controlBusOutput: QueueChannel
) {

  @GetMapping("getActiveChannels")
  fun getAllInboundChannels(): Mono<ResponseEntity<ChannelsStatusResponse>> {
    return Flux.fromIterable(
        applicationContext.getBeansWithAnnotation(InboundChannelAdapter::class.java).keys)
      .map { getChannelStatus(it) }
      .collectList()
      .map { ResponseEntity.ok().body(ChannelsStatusResponse(it)) }
  }

  @GetMapping("shutdownAll")
  fun shutdownAll(): Mono<ResponseEntity<Unit>> {
    return Flux.fromIterable(
        applicationContext.getBeansWithAnnotation(InboundChannelAdapter::class.java).keys)
      .collectList()
      .map { stopAllChannels(it) }
      .map { ResponseEntity.ok(Unit) }
  }

  fun stopAllChannels(channels: List<String>) {
    channels.forEach {
      val controllerBusMessage =
        MessageBuilder.createMessage("@${it}Endpoint.stop()", MessageHeaders(mapOf()))
      controlBusInput.send(controllerBusMessage)
    }
  }

  fun getChannelStatus(channelName: String): ChannelStatus {
    val controllerBusMessage =
      GenericMessage("@${channelName}Endpoint.isRunning()", MessageHeaders(mapOf()))
    controlBusInput.send(controllerBusMessage)
    val responseMessage = controlBusOutput.receive(1000)
    val status =
      if (responseMessage?.payload is Boolean) {
        if (responseMessage.payload == true) {
          Status.UP
        } else {
          Status.DOWN
        }
      } else {
        Status.UNKNOWN
      }
    return ChannelStatus(channelName = channelName, status = status)
  }

  class ChannelsStatusResponse(val services: List<ChannelStatus>)

  data class ChannelStatus(val channelName: String, val status: Status)

  enum class Status {
    UP,
    DOWN,
    UNKNOWN
  }
}
