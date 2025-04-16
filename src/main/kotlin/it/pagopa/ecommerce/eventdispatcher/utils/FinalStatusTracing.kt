package it.pagopa.ecommerce.eventdispatcher.utils

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.Tracer
import org.springframework.stereotype.Component

@Component
class FinalStatusTracing(private val openTelemetry: OpenTelemetry, private val tracer: Tracer) {
  companion object {
    const val TRANSACTIONID: String = "eCommerce.transactionId"
    const val TRANSACTIONEVENT: String = "eCommerce.transactionEvent"
    const val TRANSACTIONSTATUS: String = "eCommerce.transactionStatus"
    const val PSPID: String = "eCommerce.pspId"
    const val CLIENTID: String = "eCommerce.clientId"
    const val PAYMENTMETHOD: String = "eCommerce.paymentMethod"
  }

  fun addSpan(spanName: String, attributes: Attributes) {
    val span: Span = tracer.spanBuilder(spanName).startSpan()
    span.setAllAttributes(attributes)
    span.end()
  }
}
