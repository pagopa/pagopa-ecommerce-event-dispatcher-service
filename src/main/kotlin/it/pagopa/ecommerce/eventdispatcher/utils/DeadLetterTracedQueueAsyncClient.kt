package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.http.rest.Response
import com.azure.core.util.BinaryData
import com.azure.storage.queue.QueueAsyncClient
import com.azure.storage.queue.models.SendMessageResult
import io.opentelemetry.api.trace.Tracer
import it.pagopa.ecommerce.commons.domain.TransactionId
import org.springframework.beans.factory.annotation.Autowired
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Dead letter queue async client
 */
class DeadLetterTracedQueueAsyncClient(
    @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
    @Autowired private val openTelemetryTracer: Tracer
) {

    fun sendAndTraceDeadLetterQueueEvent(
        binaryData: BinaryData,
        transactionInfo: TransactionInfo,
        deadLetterQueueTTLSeconds: Int,
        category: DeadLetterErrorCategory
    ): Mono<Response<SendMessageResult>> =
        Mono.using(
            { openTelemetryTracer.spanBuilder("Event written into dead letter queue").startSpan() },
            {
                it.setAttribute("deadLetterEvent.serviceName", "pagopa-ecommerce-event-dispatcher-service")
                it.setAttribute("deadLetterEvent.category", category.toString())
                it.setAttribute(
                    "deadLetterEvent.transactionId",
                    transactionInfo.transactionId.value()
                )//solo per event dispatcher
                it.setAttribute("deadLetterEvent.transactionStatus", transactionInfo.transactionStatus)
                deadLetterQueueAsyncClient
                    .sendMessageWithResponse(
                        binaryData,
                        Duration.ZERO,
                        Duration.ofSeconds(deadLetterQueueTTLSeconds.toLong()), // timeToLive
                    )
            },
            { it.end() }
        )

    data class TransactionInfo(
        val transactionId: TransactionId,
        val transactionStatus: String
    )
}