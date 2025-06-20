package it.pagopa.ecommerce.eventdispatcher.services

import com.azure.spring.integration.storage.queue.inbound.StorageQueueMessageSource
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.ReceiverStatus
import it.pagopa.ecommerce.eventdispatcher.config.redis.bean.Status
import java.util.stream.Stream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import org.springframework.context.ApplicationContext
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.QueueChannel
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.support.MessageBuilder

class InboundChannelAdapterLifecycleHandlerServiceTest {

  private val applicationContext: ApplicationContext = mock()
  private val controlBusInput: DirectChannel = mock()
  private val controlBusOutput: QueueChannel = mock()
  private val storageMessageQueueMock: StorageQueueMessageSource = mock()

  private val messageArgumentMatcher: KArgumentCaptor<Message<Any>> = argumentCaptor()

  private val inboundChannelAdapterLifecycleHandlerService =
    InboundChannelAdapterLifecycleHandlerService(
      applicationContext = applicationContext,
      controlBusInput = controlBusInput,
      controlBusOutput = controlBusOutput)

  @Test
  fun `Should invoke command for all receiver endpoints filtering out Redis-related endpoints`() {
    // pre-requisites
    val command = "stop"
    val storageChannelAdapterBeanName = "storage"

    given(applicationContext.getBeansWithAnnotation(InboundChannelAdapter::class.java))
      .willReturn(mapOf(storageChannelAdapterBeanName to storageMessageQueueMock))

    given(controlBusInput.send(messageArgumentMatcher.capture())).willReturn(true)

    // test
    inboundChannelAdapterLifecycleHandlerService.invokeCommandForAllEndpoints(command)

    // assertions
    val capturedMessages = messageArgumentMatcher.allValues
    assertTrue(capturedMessages.size == 1)
    val capturedMessage = capturedMessages[0]
    assertEquals("@${storageChannelAdapterBeanName}Endpoint.$command()", capturedMessage.payload)
  }

  companion object {
    @JvmStatic
    fun channelStatusTestParameters(): Stream<Arguments> =
      Stream.of(
        Arguments.of(MessageBuilder.createMessage(true, MessageHeaders(mapOf())), Status.UP),
        Arguments.of(MessageBuilder.createMessage(false, MessageHeaders(mapOf())), Status.DOWN),
        Arguments.of(null, Status.UNKNOWN),
      )
  }

  @ParameterizedTest
  @MethodSource("channelStatusTestParameters")
  fun `Should retrieve channel status for all message queue excluding Redis`(
    storageStatusMessageResponse: Message<Boolean>?,
    expectedStatus: Status
  ) {
    val storageChannelAdapterBeanName = "storage"

    given(applicationContext.getBeansWithAnnotation(InboundChannelAdapter::class.java))
      .willReturn(mapOf(storageChannelAdapterBeanName to storageMessageQueueMock))

    given(controlBusInput.send(messageArgumentMatcher.capture())).willReturn(true)
    given(controlBusOutput.receive(any())).willReturn(storageStatusMessageResponse)

    val expectedReceiverStatus =
      ReceiverStatus(name = storageChannelAdapterBeanName, status = expectedStatus)

    // test
    val receiverStatusResponse = inboundChannelAdapterLifecycleHandlerService.getAllChannelStatus()

    // assertions
    val capturedMessages = messageArgumentMatcher.allValues
    assertTrue(capturedMessages.size == 1)
    val capturedMessage = capturedMessages[0]
    assertEquals("@${storageChannelAdapterBeanName}Endpoint.isRunning()", capturedMessage.payload)
    assertTrue(receiverStatusResponse.size == 1)
    val receiverStatus = receiverStatusResponse[0]
    assertEquals(expectedReceiverStatus, receiverStatus)
  }
}
