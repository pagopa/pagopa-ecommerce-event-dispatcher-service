package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherReceiverStatusTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiversStatus
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import java.time.OffsetDateTime
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * Poller class that taken into account receiver status polling. Statuses are polled as per
 * eventController.status.pollingChron parameter value and results updated into Redis document
 * uniquely identified by this module instance
 */
@Component
class EventReceiverStatusPoller(
  @Autowired
  private val eventDispatcherReceiverStatusTemplateWrapper:
    EventDispatcherReceiverStatusTemplateWrapper,
  @Autowired
  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService,
  @Autowired private val redisStreamEventControllerConfigs: RedisStreamEventControllerConfigs,
  @Value("\${eventController.deploymentVersion}")
  private val deploymentVersion: DeploymentVersionDto
) {

  private val logger = LoggerFactory.getLogger(javaClass)

  @Scheduled(cron = "\${eventController.status.pollingChron}")
  fun eventReceiverStatusPoller() {
    logger.info("Polling event receiver statuses")
    val statuses = inboundChannelAdapterLifecycleHandlerService.getAllChannelStatus()
    val instanceId = redisStreamEventControllerConfigs.instanceId
    val queriedAt = OffsetDateTime.now().toString()
    val receiversStatus =
      ReceiversStatus(
        queriedAt = queriedAt,
        receiverStatuses = statuses,
        consumerInstanceId = instanceId,
        version = deploymentVersion)
    // save new receivers status as redis instance, all records will be saved with the same key,
    // making this document to be updated automatically for each poll
    eventDispatcherReceiverStatusTemplateWrapper.save(receiversStatus)
  }
}
