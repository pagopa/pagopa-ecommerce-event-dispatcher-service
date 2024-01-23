package it.pagopa.ecommerce.eventdispatcher.controller

import it.pagopa.ecommerce.eventdispatcher.config.redis.stream.EventDispatcherCommandsTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.stream.RedisStreamMessageSource
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.ApplicationContext
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.http.ResponseEntity
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.QueueChannel
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.GenericMessage
import org.springframework.messaging.support.MessageBuilder
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
class CommandController(
  @Autowired private val applicationContext: ApplicationContext,
  @Autowired @Qualifier("controlBusInCH") private val controlBusInput: DirectChannel,
  @Autowired @Qualifier("controlBusOutCH") private val controlBusOutput: QueueChannel,
  @Autowired
  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper
) {

  @GetMapping("channels")
  fun getAllInboundChannels(): Mono<ResponseEntity<ChannelsStatusResponse>> {
    return Flux.fromIterable(findInboundChannelAdapterBeans())
      .map { getChannelStatus(it) }
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
    return Flux.fromIterable(findInboundChannelAdapterBeans())
      .collectList()
      .map { invokeCommandForAllEndpoints(it, "stop") }
      .map { ResponseEntity.ok(Unit) }
  }

  @PostMapping("channels/start")
  fun startAll(): Mono<ResponseEntity<Unit>> {
    return Flux.fromIterable(findInboundChannelAdapterBeans())
      .collectList()
      .map { invokeCommandForAllEndpoints(it, "start") }
      .map { ResponseEntity.ok(Unit) }
  }

  fun invokeCommandForAllEndpoints(channels: List<String>, command: String) {
    channels.forEach {
      val controllerBusMessage =
        MessageBuilder.createMessage("@${it}Endpoint.$command()", MessageHeaders(mapOf()))
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

  fun findInboundChannelAdapterBeans() =
    applicationContext
      .getBeansWithAnnotation(InboundChannelAdapter::class.java)
      .filterNot { it.value is RedisStreamMessageSource }
      .keys

  class ChannelsStatusResponse(val services: List<ChannelStatus>)

  data class ChannelStatus(val channelName: String, val status: Status)

  enum class Status {
    UP,
    DOWN,
    UNKNOWN
  }
}
