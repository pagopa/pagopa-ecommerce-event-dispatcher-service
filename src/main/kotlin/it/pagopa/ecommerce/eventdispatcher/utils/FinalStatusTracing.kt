package it.pagopa.ecommerce.eventdispatcher.utils

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import org.springframework.stereotype.Component

@Component
class FinalStatusTracing(private val openTelemetry: OpenTelemetry, private val tracer: Tracer) {
    companion object {
        const val TRANSACTIONID: String = "transactionId"
        const val PSPID: String = "pspId"
        const val CLIENTID: String = "clientId"
        const val PAYMENTMETHOD: String = "paymentMethod"
    }

    fun addSpan(spanName: String, attributes: Attributes) {
        val span: Span =
            tracer
                .spanBuilder(spanName)
                .setParent(Context.current().with(Span.current()))
                .startSpan()
        span.setAllAttributes(attributes)
        span.end()
    }
}