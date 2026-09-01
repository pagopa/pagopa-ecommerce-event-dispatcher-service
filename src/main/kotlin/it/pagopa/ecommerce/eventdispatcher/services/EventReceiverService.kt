package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.commons.mdcutilities.LogTracingUtils
import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherCommandsTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherReceiverStatusTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoEventReceiverStatusFound
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.generated.eventdispatcher.server.model.*
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/** This class handles all InboundChannelsAdapters events receivers */
@Service
class EventReceiverService(
  @Autowired
  private val eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper,
  @Autowired
  private val eventDispatcherReceiverStatusTemplateWrapper:
    EventDispatcherReceiverStatusTemplateWrapper,
  @Autowired private val redisStreamConf: RedisStreamEventControllerConfigs
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  suspend fun handleCommand(eventReceiverCommandRequestDto: EventReceiverCommandRequestDto) {
    val commandToSend =
      when (eventReceiverCommandRequestDto.command) {
        EventReceiverCommandRequestDto.Command.START ->
          EventDispatcherReceiverCommand.ReceiverCommand.START
        EventReceiverCommandRequestDto.Command.STOP ->
          EventDispatcherReceiverCommand.ReceiverCommand.STOP
      }
    // trim all events before adding new event to be processed
    val recordId =
      eventDispatcherCommandsTemplateWrapper
        .writeEventToStreamTrimmingEvents(
          redisStreamConf.streamKey,
          EventDispatcherReceiverCommand(
            receiverCommand = commandToSend,
            version = eventReceiverCommandRequestDto.deploymentVersion),
          0)
        .awaitSingle()
    LogTracingUtils.loggerTracingUtils()
      .success()
      .details(
        mapOf(
          "command" to commandToSend.toString(),
          "record_id" to recordId.toString(),
        ))
      .logInfo(logger, "Sent new event to Redis stream")
  }

  suspend fun getReceiversStatus(
    deploymentVersionDto: DeploymentVersionDto?
  ): EventReceiverStatusResponseDto {
    return eventDispatcherReceiverStatusTemplateWrapper.allValuesInKeySpace
      .filter {
        if (deploymentVersionDto != null) {
          it.version == deploymentVersionDto
        } else {
          true
        }
      }
      .map { receiverStatuses ->
        EventReceiverStatusDto(
          receiverStatuses =
            receiverStatuses.receiverStatuses.map { receiverStatus ->
              ReceiverStatusDto(
                status =
                  receiverStatus.status.let { ReceiverStatusDto.Status.valueOf(it.toString()) },
                name = receiverStatus.name)
            },
          instanceId = receiverStatuses.consumerInstanceId,
          deploymentVersion = receiverStatuses.version)
      }
      .switchIfEmpty { throw NoEventReceiverStatusFound() }
      .collectList()
      .map { EventReceiverStatusResponseDto(status = it) }
      .awaitSingle()
  }
}
