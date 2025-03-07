package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherCommandsTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherReceiverStatusTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiverStatus
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiversStatus
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.Status
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoEventReceiverStatusFound
import it.pagopa.generated.eventdispatcher.server.model.*
import java.time.OffsetDateTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.*
import org.springframework.data.redis.connection.stream.RecordId

@OptIn(ExperimentalCoroutinesApi::class)
class EventReceiversServiceTest {

  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper =
    mock()

  private val eventDispatcherReceiverStatusTemplateWrapper:
    EventDispatcherReceiverStatusTemplateWrapper =
    mock()

  private val redisStreamConf: RedisStreamEventControllerConfigs =
    RedisStreamEventControllerConfigs(streamKey = "streamKey")

  private val eventReceiverService =
    EventReceiverService(
      eventDispatcherCommandsTemplateWrapper = eventDispatcherCommandsTemplateWrapper,
      eventDispatcherReceiverStatusTemplateWrapper = eventDispatcherReceiverStatusTemplateWrapper,
      redisStreamConf = redisStreamConf)

  @ParameterizedTest
  @EnumSource(EventReceiverCommandRequestDto.Command::class)
  fun `Should handle receiver command successfully`(
    eventCommand: EventReceiverCommandRequestDto.Command
  ) = runTest {
    // pre-requisites
    val eventReceiverCommandDto = EventReceiverCommandRequestDto(command = eventCommand)
    given(
        eventDispatcherCommandsTemplateWrapper.writeEventToStreamTrimmingEvents(
          any(), any(), any()))
      .willReturn(RecordId.autoGenerate())
    // test
    eventReceiverService.handleCommand(eventReceiverCommandDto)
    // assertions
    verify(eventDispatcherCommandsTemplateWrapper, times(1))
      .writeEventToStreamTrimmingEvents(
        eq(redisStreamConf.streamKey),
        argThat {
          assertEquals(eventReceiverCommandDto.command.toString(), this.receiverCommand.toString())
          true
        },
        eq(0))
  }

  @Test
  fun `Should get receiver statuses successfully`() = runTest {
    // pre-requisites
    val instanceId = "instanceId"
    val receiverName = "receiverName"
    val receiverStatus = "UP"
    val receiverStatuses =
      mutableListOf(
        ReceiversStatus(
          receiverStatuses =
            listOf(ReceiverStatus(name = receiverName, status = Status.valueOf(receiverStatus))),
          consumerInstanceId = instanceId,
          version = DeploymentVersionDto.PROD,
          queriedAt = OffsetDateTime.now().toString()))
    given(eventDispatcherReceiverStatusTemplateWrapper.allValuesInKeySpace)
      .willReturn(receiverStatuses)
    val expectedResponse =
      EventReceiverStatusResponseDto(
        status =
          listOf(
            EventReceiverStatusDto(
              receiverStatuses =
                listOf(
                  ReceiverStatusDto(
                    name = receiverName,
                    status = ReceiverStatusDto.Status.valueOf(receiverStatus))),
              deploymentVersion = DeploymentVersionDto.PROD,
              instanceId = instanceId)))
    // test
    val response = eventReceiverService.getReceiversStatus(null)

    // assertions
    verify(eventDispatcherReceiverStatusTemplateWrapper, times(1)).allValuesInKeySpace
    assertEquals(expectedResponse, response)
  }

  @Test
  fun `Should throw NoEventReceiverStatusFound for no receiver status found into redis`() =
    runTest {
      // pre-requisites

      given(eventDispatcherReceiverStatusTemplateWrapper.allValuesInKeySpace).willReturn(listOf())

      // test
      assertThrows<NoEventReceiverStatusFound> { eventReceiverService.getReceiversStatus(null) }

      // assertions
      verify(eventDispatcherReceiverStatusTemplateWrapper, times(1)).allValuesInKeySpace
    }

  @Test
  fun `Should get receiver statuses filtering for specific version`() = runTest {
    // pre-requisites
    val instanceId = "instanceId"
    val receiverName = "receiverName"
    val receiverStatus = "UP"
    val receiverStatuses =
      mutableListOf(
        ReceiversStatus(
          receiverStatuses =
            listOf(ReceiverStatus(name = receiverName, status = Status.valueOf(receiverStatus))),
          consumerInstanceId = instanceId,
          version = DeploymentVersionDto.PROD,
          queriedAt = OffsetDateTime.now().toString()),
        ReceiversStatus(
          receiverStatuses =
            listOf(ReceiverStatus(name = receiverName, status = Status.valueOf(receiverStatus))),
          consumerInstanceId = instanceId,
          version = DeploymentVersionDto.STAGING,
          queriedAt = OffsetDateTime.now().toString()))
    given(eventDispatcherReceiverStatusTemplateWrapper.allValuesInKeySpace)
      .willReturn(receiverStatuses)
    val expectedResponse =
      EventReceiverStatusResponseDto(
        status =
          listOf(
            EventReceiverStatusDto(
              receiverStatuses =
                listOf(
                  ReceiverStatusDto(
                    name = receiverName,
                    status = ReceiverStatusDto.Status.valueOf(receiverStatus))),
              deploymentVersion = DeploymentVersionDto.PROD,
              instanceId = instanceId)))
    // test
    val response = eventReceiverService.getReceiversStatus(DeploymentVersionDto.PROD)

    // assertions
    verify(eventDispatcherReceiverStatusTemplateWrapper, times(1)).allValuesInKeySpace
    assertEquals(expectedResponse, response)
  }
}
