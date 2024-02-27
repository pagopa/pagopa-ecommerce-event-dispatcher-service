package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationOutcomeWaitingEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.nio.charset.StandardCharsets
import kotlinx.coroutines.reactor.mono
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class TransactionAuthorizationRequestedRetriedQueueConsumerTest {

  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionAuthorizationRequestedRetryQueueConsumer =
    mock()
  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  val strictSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val queueConsumerV2Captor:
    KArgumentCaptor<
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>> =
    argumentCaptor<
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>>()

  private val checkpointer: Checkpointer = mock()

  private val transactionAuthorizationRequestedRetryQueueConsumer =
    spy(
      TransactionAuthorizationRequestedRetryQueueConsumer(
        queueConsumerV2 = queueConsumerV2,
        deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
        strictSerializerProviderV2 = strictSerializerProviderV2))

  @Test
  fun `Should dispatch authorization requested events`() {
    // pre-condition
    val originalEvent = TransactionTestUtils.transactionAuthorizationOutcomeWaitingEvent(0)
    val serializedEvent =
      String(
        BinaryData.fromObject(
            QueueEvent(originalEvent, TracingInfoTest.MOCK_TRACING_INFO),
            strictSerializerProviderV2.createInstance())
          .toBytes(),
        StandardCharsets.UTF_8)
    println("Serialized event: $serializedEvent")
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          serializedEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .verifyComplete()
    // assertions
    verify(queueConsumerV2, times(1)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any())
    val queueEvent = queueConsumerV2Captor.firstValue
    val actualEvent = queueEvent.fold({ it }, { it })
    Assertions.assertEquals(originalEvent, actualEvent.event)
    Assertions.assertNotNull(actualEvent.tracingInfo)
  }

  @Test
  fun `Should write event to dead letter queue for invalid event received`() {
    // pre-condition
    val invalidEvent = "test"
    val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectNext(Unit)
      .verifyComplete()
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
  }

  @Test
  fun `Should handle error writing event to dead letter queue for invalid event received`() {
    // pre-condition
    val invalidEvent = "test"
    val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(Mono.error(RuntimeException("Error writing event to queue")))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
  }

  @Test
  fun `Should not dispatch events different authorization requested retry events`() {
    // pre-condition
    val authCompletedEvent =
      TransactionTestUtils.transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData())
    val serializedEvent =
      String(
        BinaryData.fromObject(
            QueueEvent(authCompletedEvent, TracingInfoTest.MOCK_TRACING_INFO),
            strictSerializerProviderV2.createInstance())
          .toBytes(),
        StandardCharsets.UTF_8)
    println("Serialized event: $serializedEvent")
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          serializedEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectError(InvalidEventException::class.java)
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(0))
      .sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any())
  }

  @Test
  fun `Should write event to dead letter queue for error checkpointing event`() {
    // pre-condition
    val invalidEvent = "test"
    val binaryData = BinaryData.fromBytes(invalidEvent.toByteArray(StandardCharsets.UTF_8))
    given(queueConsumerV2.messageReceiver(queueConsumerV2Captor.capture(), any()))
      .willReturn(Mono.empty())
    given(checkpointer.success())
      .willReturn(Mono.error(RuntimeException("Error checkpointing event")))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(Mono.error(RuntimeException("Error writing event to queue")))
    // test
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionAuthorizationRequestedRetryQueueConsumer.messageReceiver(
          invalidEvent.toByteArray(StandardCharsets.UTF_8), checkpointer))
      .expectError(java.lang.RuntimeException::class.java)
      .verify()
    // assertions
    verify(queueConsumerV2, times(0)).messageReceiver(any(), any())
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> { this.toString() == binaryData.toString() },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = null,
            transactionEventCode = null,
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.EVENT_PARSING_ERROR)))
  }
}
