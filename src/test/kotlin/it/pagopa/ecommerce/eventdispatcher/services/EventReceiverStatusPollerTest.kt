package it.pagopa.ecommerce.eventdispatcher.services

import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.config.redis.EventDispatcherReceiverStatusTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiverStatus
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.Status
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*

class EventReceiverStatusPollerTest {

  private val inboundChannelAdapterHandlerService: InboundChannelAdapterHandlerService = mock()

  private val eventDispatcherReceiverStatusTemplateWrapper:
    EventDispatcherReceiverStatusTemplateWrapper =
    mock()

  private val redisStreamEventControllerConfigs =
    RedisStreamEventControllerConfigs(
      streamKey = "streamKey",
      consumerGroupPrefix = "consumerPrefix",
      consumerNamePrefix = "consumerName",
      faiOnErrorCreatingConsumerGroup = false)

  private val eventReceiverStatusPoller =
    EventReceiverStatusPoller(
      inboundChannelAdapterHandlerService = inboundChannelAdapterHandlerService,
      redisStreamEventControllerConfigs = redisStreamEventControllerConfigs,
      eventDispatcherReceiverStatusTemplateWrapper = eventDispatcherReceiverStatusTemplateWrapper)

  @Test
  fun `Should poll for status successfully saving receiver statuses`() {
    // assertions
    val receiverStatuses =
      listOf(
        ReceiverStatus(name = "receiver1", status = Status.UP),
        ReceiverStatus(name = "receiver2", status = Status.DOWN))
    given(inboundChannelAdapterHandlerService.getAllChannelStatus()).willReturn(receiverStatuses)
    doNothing().`when`(eventDispatcherReceiverStatusTemplateWrapper).save(any())
    // test
    eventReceiverStatusPoller.eventReceiverStatusPoller()
    verify(eventDispatcherReceiverStatusTemplateWrapper, times(1))
      .save(
        argThat {
          assertEquals(redisStreamEventControllerConfigs.consumerName, this.consumerInstanceId)
          assertEquals(receiverStatuses, this.receiverStatuses)
          true
        })
  }
}
