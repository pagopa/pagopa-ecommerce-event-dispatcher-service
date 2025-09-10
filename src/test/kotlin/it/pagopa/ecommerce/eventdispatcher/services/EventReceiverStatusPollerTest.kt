package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherReceiverStatusTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiverStatus
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.Status
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import reactor.core.publisher.Mono

class EventReceiverStatusPollerTest {

  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService =
    mock()

  private val eventDispatcherReceiverStatusTemplateWrapper:
    EventDispatcherReceiverStatusTemplateWrapper =
    mock()

  private val redisStreamEventControllerConfigs =
    RedisStreamEventControllerConfigs(streamKey = "streamKey")

  private val eventReceiverStatusPoller =
    EventReceiverStatusPoller(
      inboundChannelAdapterLifecycleHandlerService = inboundChannelAdapterLifecycleHandlerService,
      redisStreamEventControllerConfigs = redisStreamEventControllerConfigs,
      eventDispatcherReceiverStatusTemplateWrapper = eventDispatcherReceiverStatusTemplateWrapper,
      deploymentVersion = DeploymentVersionDto.PROD)

  @Test
  fun `Should poll for status successfully saving receiver statuses`() = runTest {
    // assertions
    val receiverStatuses =
      listOf(
        ReceiverStatus(name = "receiver1", status = Status.UP),
        ReceiverStatus(name = "receiver2", status = Status.DOWN))
    given(inboundChannelAdapterLifecycleHandlerService.getAllChannelStatus())
      .willReturn(receiverStatuses)
    given(eventDispatcherReceiverStatusTemplateWrapper.save(any())).willReturn(Mono.just(true))
    // test
    eventReceiverStatusPoller.eventReceiverStatusPoller()
    verify(eventDispatcherReceiverStatusTemplateWrapper, times(1))
      .save(
        argThat {
          assertEquals(receiverStatuses, this.receiverStatuses)
          true
        })
  }
}
