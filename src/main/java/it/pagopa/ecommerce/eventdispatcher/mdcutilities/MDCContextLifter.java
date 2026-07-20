package it.pagopa.ecommerce.eventdispatcher.mdcutilities;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.reactivestreams.Subscription;
import org.slf4j.MDC;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * Helper that copies selected values from Reactor Context to MDC for each
 * signal.
 */
class MDCContextLifter<T> implements CoreSubscriber<T> {

    CoreSubscriber<T> coreSubscriber;

    public MDCContextLifter(CoreSubscriber<T> coreSubscriber) {
        this.coreSubscriber = coreSubscriber;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        coreSubscriber.onSubscribe(subscription);
    }

    @Override
    public void onNext(T obj) {
        copyToMdc(coreSubscriber.currentContext());
        coreSubscriber.onNext(obj);
    }

    @Override
    public void onError(Throwable t) {
        try {
            copyToMdc(coreSubscriber.currentContext());
            coreSubscriber.onError(t);
        } finally {
            MDC.clear();
        }
    }

    @Override
    public void onComplete() {
        try {
            copyToMdc(coreSubscriber.currentContext());
            coreSubscriber.onComplete();
        } finally {
            MDC.clear();
        }
    }

    @Override
    public Context currentContext() {
        return coreSubscriber.currentContext();
    }

    private void copyToMdc(Context context) {
        if (!context.isEmpty()) {
            Map<String, String> mdcContextMap = Optional.ofNullable(MDC.getCopyOfContextMap()).orElseGet(HashMap::new);
            Map<String, String> reactorContextMap = Arrays
                    .stream(EventDispatcherTracingUtils.TracingEntry.values())
                    .filter(EventDispatcherTracingUtils.TracingEntry::isContextBound)
                    .map(
                            key -> new AbstractMap.SimpleEntry<>(
                                    key.getKey(),
                                    String.valueOf(
                                            context.getOrDefault(key.getKey(), key.getDefaultValue())
                                    )
                            )
                    )
                    .collect(
                            Collectors.toMap(
                                    AbstractMap.SimpleEntry::getKey,
                                    AbstractMap.SimpleEntry::getValue
                            )
                    );
            mdcContextMap.putAll(reactorContextMap);
            MDC.setContextMap(mdcContextMap);
        } else {
            MDC.clear();
        }
    }
}
