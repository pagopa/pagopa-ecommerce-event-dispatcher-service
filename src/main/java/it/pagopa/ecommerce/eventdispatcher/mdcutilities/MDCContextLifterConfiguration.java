package it.pagopa.ecommerce.cdc.mdcutilities;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;

@Configuration
public class MDCContextLifterConfiguration {

    private final String mdcContextReactorKey = MDCContextLifterConfiguration.class.getName();

    @PostConstruct
    private void contextOperatorHook() {
        Hooks.onEachOperator(
                mdcContextReactorKey,
                Operators.lift(
                        (
                         scannable,
                         coreSubscriber
                        ) -> new MDCContextLifter<>(coreSubscriber)
                )
        );
    }

    @PreDestroy
    private void cleanupHook() {
        Hooks.resetOnEachOperator(mdcContextReactorKey);
    }
}
