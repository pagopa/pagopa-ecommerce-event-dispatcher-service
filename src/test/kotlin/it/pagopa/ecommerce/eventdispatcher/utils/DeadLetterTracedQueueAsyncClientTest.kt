package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.util.BinaryData
import com.azure.storage.queue.QueueAsyncClient
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.domain.TransactionId
import java.time.Duration
import java.util.*
import java.util.stream.Stream
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

class DeadLetterTracedQueueAsyncClientTest {

  private val openTelemetryTracer: Tracer = mock()
  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()
  private val deadLetterTTLSeconds = 0
  private val spanBuilder: SpanBuilder = mock()
  private val span: Span = mock()
  private val deadLetterTracedQueueAsyncClient =
    DeadLetterTracedQueueAsyncClient(
      deadLetterQueueAsyncClient, deadLetterTTLSeconds, openTelemetryTracer)

  companion object {
    @JvmStatic
    fun errorCategoryMethodSource(): Stream<Arguments> =
      Arrays.stream(DeadLetterTracedQueueAsyncClient.ErrorCategory.values()).map {
        Arguments.of(it)
      }
  }

  @ParameterizedTest
  @MethodSource("errorCategoryMethodSource")
  fun `Should write event to dead letter queue creating span`(
    errorCategory: DeadLetterTracedQueueAsyncClient.ErrorCategory
  ) {
    // pre-requisites
    val binaryData = BinaryData.fromBytes("test".toByteArray())
    val eventCode = "EVENT_CODE"
    val transactionId = TransactionId(UUID.randomUUID())
    val errorContext =
      DeadLetterTracedQueueAsyncClient.ErrorContext(
        transactionId = transactionId,
        transactionEventCode = eventCode,
        errorCategory = errorCategory)
    given(openTelemetryTracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)
    given(span.setAttribute(any<String>(), any<String>())).willReturn(span)
    doNothing().`when`(span).end()
    given(
        deadLetterQueueAsyncClient.sendMessageWithResponse(
          any<BinaryData>(),
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong()))))
      .willReturn(queueSuccessfulResponse())
    // test
    StepVerifier.create(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(binaryData, errorContext))
      .expectNext(Unit)
      .verifyComplete()
    verify(openTelemetryTracer, times(1)).spanBuilder(any())
    verify(spanBuilder, times(1)).startSpan()
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.serviceName")),
        eq("pagopa-ecommerce-event-dispatcher-service"))
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.category")), eq(errorCategory.toString()))
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.transactionId")), eq(transactionId.value()))
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.transactionEventCode")), eq(eventCode))
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        eq(binaryData), eq(Duration.ZERO), eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
  }

  @Test
  fun `Should write event to dead letter queue creating span without transactionId and transactionEventCode information`() {
    // pre-requisites
    val binaryData = BinaryData.fromBytes("test".toByteArray())
    val eventCode = null
    val transactionId = null
    val errorContext =
      DeadLetterTracedQueueAsyncClient.ErrorContext(
        transactionId = transactionId,
        transactionEventCode = eventCode,
        errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)
    given(openTelemetryTracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)
    given(span.setAttribute(any<String>(), any<String>())).willReturn(span)
    doNothing().`when`(span).end()
    given(
        deadLetterQueueAsyncClient.sendMessageWithResponse(
          any<BinaryData>(),
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong()))))
      .willReturn(queueSuccessfulResponse())
    // test
    StepVerifier.create(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(binaryData, errorContext))
      .expectNext(Unit)
      .verifyComplete()
    verify(openTelemetryTracer, times(1)).spanBuilder(any())
    verify(spanBuilder, times(1)).startSpan()
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.serviceName")),
        eq("pagopa-ecommerce-event-dispatcher-service"))
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.category")),
        eq(DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR.toString()))
    verify(span, times(1))
      .setAttribute(eq(AttributeKey.stringKey("deadLetterEvent.transactionId")), eq("N/A"))
    verify(span, times(1))
      .setAttribute(eq(AttributeKey.stringKey("deadLetterEvent.transactionEventCode")), eq("N/A"))
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        eq(binaryData), eq(Duration.ZERO), eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
  }

  @Test
  fun `Should return mono error for error writing event to dead letter queue`() {
    // pre-requisites
    val binaryData = BinaryData.fromBytes("test".toByteArray())
    val eventCode = null
    val transactionId = null
    val errorContext =
      DeadLetterTracedQueueAsyncClient.ErrorContext(
        transactionId = transactionId,
        transactionEventCode = eventCode,
        errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)
    given(openTelemetryTracer.spanBuilder(any())).willReturn(spanBuilder)
    given(spanBuilder.startSpan()).willReturn(span)
    given(span.setAttribute(any<String>(), any<String>())).willReturn(span)
    doNothing().`when`(span).end()
    given(
        deadLetterQueueAsyncClient.sendMessageWithResponse(
          any<BinaryData>(),
          eq(Duration.ZERO),
          eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong()))))
      .willReturn(Mono.error(RuntimeException("Error writing event to dead letter queue")))
    // test
    StepVerifier.create(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(binaryData, errorContext))
      .expectError(RuntimeException::class.java)
      .verify()
    verify(openTelemetryTracer, times(1)).spanBuilder(any())
    verify(spanBuilder, times(1)).startSpan()
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.serviceName")),
        eq("pagopa-ecommerce-event-dispatcher-service"))
    verify(span, times(1))
      .setAttribute(
        eq(AttributeKey.stringKey("deadLetterEvent.category")),
        eq(DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR.toString()))
    verify(span, times(1))
      .setAttribute(eq(AttributeKey.stringKey("deadLetterEvent.transactionId")), eq("N/A"))
    verify(span, times(1))
      .setAttribute(eq(AttributeKey.stringKey("deadLetterEvent.transactionEventCode")), eq("N/A"))
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        eq(binaryData), eq(Duration.ZERO), eq(Duration.ofSeconds(deadLetterTTLSeconds.toLong())))
  }
}
