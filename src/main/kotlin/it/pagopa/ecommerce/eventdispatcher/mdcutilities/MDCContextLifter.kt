package it.pagopa.ecommerce.eventdispatcher.mdcutilities

import org.reactivestreams.Subscription
import org.slf4j.MDC
import reactor.core.CoreSubscriber
import reactor.util.context.Context

/**
 * MDC Context Lifter for transaction IDs
 */
class MDCContextLifter<T>(private val coreSubscriber: CoreSubscriber<T>) : CoreSubscriber<T> {

    companion object {
        const val TRANSACTION_ID_KEY = "transactionId"
    }

    override fun onSubscribe(subscription: Subscription) {
        coreSubscriber.onSubscribe(subscription)
    }

    override fun onNext(obj: T) {
        copyToMdc(coreSubscriber.currentContext())
        try {
            coreSubscriber.onNext(obj)
        } finally {
            MDC.clear()
        }
    }

    override fun onError(t: Throwable) {
        copyToMdc(coreSubscriber.currentContext())
        try {
            coreSubscriber.onError(t)
        } finally {
            MDC.clear()
        }
    }

    override fun onComplete() {
        copyToMdc(coreSubscriber.currentContext())
        try {
            coreSubscriber.onComplete()
        } finally {
            MDC.clear()
        }
    }

    override fun currentContext(): Context {
        return coreSubscriber.currentContext()
    }

    /**
     * Copies the transaction ID from Reactor context to MDC
     */
    private fun copyToMdc(context: Context) {
        if (!context.isEmpty) {
            context.getOrEmpty<String>(TRANSACTION_ID_KEY).ifPresent { transactionId ->
                MDC.put(TRANSACTION_ID_KEY, transactionId)
            }
        }
    }
}