package it.pagopa.ecommerce.eventdispatcher.mdcutilities

import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * Utility for adding transaction ID to MDC context
 */
@Component
class MdcTransactionHelper {

    /**
     * Adds transaction ID to context for a pipeline
     */
    fun <T> addTransactionIdToContext(transactionId: String, pipeline: Mono<T>): Mono<T> {
        return pipeline.contextWrite { ctx ->
            ctx.put(MDCContextLifter.TRANSACTION_ID_KEY, transactionId)
        }
    }
}