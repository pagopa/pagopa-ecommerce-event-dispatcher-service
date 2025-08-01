package it.pagopa.ecommerce.eventdispatcher.redis.streams

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherCommandMixin
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand.CommandType
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.*
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.stream.StreamReceiver
import reactor.core.publisher.Flux
import reactor.test.StepVerifier

class RedisStreamConsumerTest {

  private val deploymentVersion = DeploymentVersionDto.PROD

  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService =
    mock()

  private val redisStreamReceiver:
    StreamReceiver<String, ObjectRecord<String, LinkedHashMap<*, *>>> =
    mock()
  private val redisStreamConf: RedisStreamEventControllerConfigs = mock()

  private val redisStreamConsumer =
    RedisStreamConsumer(
      redisStreamReceiver = redisStreamReceiver,
      redisStreamConf = redisStreamConf,
      inboundChannelAdapterLifecycleHandlerService = inboundChannelAdapterLifecycleHandlerService,
      deploymentVersion = deploymentVersion)

  private val objectMapper: ObjectMapper = mock()

  private val applicationReadyEvent: ApplicationReadyEvent = mock()

  private fun createMockRecord(): ObjectRecord<String, LinkedHashMap<*, *>> {
    val record: ObjectRecord<String, LinkedHashMap<*, *>> = mock()
    val value: LinkedHashMap<String, Any> = LinkedHashMap()
    whenever(record.value).thenReturn(value as LinkedHashMap<*, *>)
    return record
  }

  @ParameterizedTest
  @EnumSource(EventDispatcherReceiverCommand.ReceiverCommand::class)
  fun `Should handle receiver event successfully`(
    command: EventDispatcherReceiverCommand.ReceiverCommand
  ) {
    // pre-requisite
    val eventMessage =
      EventDispatcherReceiverCommand(receiverCommand = command, version = DeploymentVersionDto.PROD)
    // test
    redisStreamConsumer.handleEventReceiverCommand(eventMessage)
    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService, times(1))
      .invokeCommandForAllEndpoints(command.toString().lowercase())
  }

  @Test
  fun `Should throw exception for unmanaged event`() {
    // pre-requisite
    val invalidEvent: EventDispatcherCommandMixin = mock()
    whenever(invalidEvent.type).thenReturn(CommandType.RECEIVER_COMMAND)
    // test
    assertThrows<RuntimeException> { redisStreamConsumer.processStreamEvent(invalidEvent) }

    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService, times(0))
      .invokeCommandForAllEndpoints(any())
  }

  @ParameterizedTest
  @EnumSource(EventDispatcherReceiverCommand.ReceiverCommand::class)
  fun `Should ignore event for command target version different than current version`(
    command: EventDispatcherReceiverCommand.ReceiverCommand
  ) {
    // pre-requisite
    val version = DeploymentVersionDto.values().first { it != deploymentVersion }
    val eventMessage = EventDispatcherReceiverCommand(receiverCommand = command, version = version)

    // test
    redisStreamConsumer.handleEventReceiverCommand(eventMessage)

    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService, times(0))
      .invokeCommandForAllEndpoints(any())
  }

  @Test
  fun `Should set up event stream pipeline and subscribe on application ready`() {
    // pre-requisite
    val mockFlux = Flux.empty<ObjectRecord<String, LinkedHashMap<*, *>>>()
    val spyRedisStreamConsumer = spy(redisStreamConsumer)
    doReturn(mockFlux).`when`(spyRedisStreamConsumer).eventStreamPipelineWithRetry()
    // test
    spyRedisStreamConsumer.onApplicationEvent(applicationReadyEvent)
    // assertions
    verify(spyRedisStreamConsumer).eventStreamPipelineWithRetry()
  }

  @Test
  fun `Should handle exceptions when processing stream events`() {
    // pre-requisite
    val mockRecord = createMockRecord()
    val mockFlux = Flux.just(mockRecord)
    val spyRedisStreamConsumer = spy(redisStreamConsumer)
    doReturn(mockFlux).`when`(spyRedisStreamConsumer).eventStreamPipelineWithRetry()

    val field = RedisStreamConsumer::class.java.getDeclaredField("objectMapper")
    field.isAccessible = true
    val originalMapper = field.get(spyRedisStreamConsumer)

    try {
      field.set(spyRedisStreamConsumer, objectMapper)
      whenever(objectMapper.convertValue(any(), eq(EventDispatcherGenericCommand::class.java)))
        .thenThrow(RuntimeException("Test exception"))

      // test
      spyRedisStreamConsumer.onApplicationEvent(applicationReadyEvent)

      // assertions
      verify(spyRedisStreamConsumer, never()).processStreamEvent(any())
    } finally {
      field.set(spyRedisStreamConsumer, originalMapper)
    }
  }

  @Test
  fun `Should handle exceptions in Redis stream pipeline`() {
    // pre-requisite
    val mockFlux =
      Flux.error<ObjectRecord<String, LinkedHashMap<*, *>>>(
        RuntimeException("Stream pipeline error"))
    val spyRedisStreamConsumer = spy(redisStreamConsumer)
    doReturn(mockFlux).`when`(spyRedisStreamConsumer).eventStreamPipelineWithRetry()
    // test
    spyRedisStreamConsumer.onApplicationEvent(applicationReadyEvent)
    // assertions
    verify(spyRedisStreamConsumer).eventStreamPipelineWithRetry()
  }

  @Test
  fun `Should create Redis stream event pipeline with retry`() {
    // pre-requisite
    val mockStream = "mock-stream"
    whenever(redisStreamConf.streamKey).thenReturn(mockStream)
    val mockRecord = createMockRecord()
    val mockFlux = Flux.just(mockRecord)
    whenever(redisStreamReceiver.receive(any())).thenReturn(mockFlux)
    // test
    val resultFlux = redisStreamConsumer.eventStreamPipelineWithRetry()
    // assertions
    StepVerifier.create(resultFlux.take(1)).expectNextCount(1).verifyComplete()

    verify(redisStreamReceiver).receive(any())
  }

  @Test
  fun `Should handle onFailure when processStreamEvent throws exception`() {
    // pre-requisite
    val processStreamEventLambda = { record: ObjectRecord<String, LinkedHashMap<*, *>> ->
      runCatching {
          val event = mock<EventDispatcherGenericCommand>()
          redisStreamConsumer.processStreamEvent(event)
        }
        .onFailure { ex ->
          assertTrue(ex is RuntimeException, "Exception should be RuntimeException")
        }
    }
    val mockRecord = createMockRecord()
    val spyRedisStreamConsumer = spy(redisStreamConsumer)
    doThrow(RuntimeException("Test exception"))
      .`when`(spyRedisStreamConsumer)
      .processStreamEvent(any())

    // test
    processStreamEventLambda(mockRecord)
  }

  @Test
  fun `Should handle STOP command correctly`() {
    // pre-requisite
    val command = EventDispatcherReceiverCommand.ReceiverCommand.STOP
    val event =
      EventDispatcherReceiverCommand(receiverCommand = command, version = deploymentVersion)
    // test
    redisStreamConsumer.handleEventReceiverCommand(event)
    // assertion
    verify(inboundChannelAdapterLifecycleHandlerService).invokeCommandForAllEndpoints("stop")
  }

  @Test
  fun `Should handle command with null version`() {
    // pre-requisite
    val command = EventDispatcherReceiverCommand.ReceiverCommand.START
    val event = EventDispatcherReceiverCommand(receiverCommand = command, version = null)
    // test
    redisStreamConsumer.handleEventReceiverCommand(event)
    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService).invokeCommandForAllEndpoints("start")
  }

  @Test
  fun `Should handle command with matching version`() {
    // pre-requisite
    val matchingVersion = deploymentVersion
    val command = EventDispatcherReceiverCommand.ReceiverCommand.START
    val event = EventDispatcherReceiverCommand(receiverCommand = command, version = matchingVersion)
    // test
    redisStreamConsumer.handleEventReceiverCommand(event)
    // assertions
    verify(inboundChannelAdapterLifecycleHandlerService).invokeCommandForAllEndpoints("start")
  }
}
