package it.pagopa.ecommerce.cdc.mdcutilities;

import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent;
import reactor.util.context.Context;

/**
 * Utility class with helper methods to enrich Reactor Context for CDC event
 * processing.
 */
public class CdcTracingUtils {

    private CdcTracingUtils() {
    }

    /** Tracing keys copied from Reactor Context to MDC. */
    public enum TracingEntry {
        CTX_TRANSACTION_ID("ctx.transaction.id", "{transactionId-not-found}"),
        CTX_EVENT_CODE("ctx.event.code", "{eventCode-not-found}"),
        CTX_EVENT_ID("ctx.event.id", "{eventId-not-found}"),
        EVENT_ACTION("event.action", "{eventAction-not-found}");

        private final String key;
        private final String defaultValue;

        TracingEntry(
                String key,
                String defaultValue
        ) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        public String getKey() {
            return key;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }

    /** Enrich Reactor Context with CDC event metadata used by MDC/logging hooks. */
    public static Context enrichContextForCdcEvent(
                                                   TransactionEvent<?> event,
                                                   Context reactorContext
    ) {
        return reactorContext
                .put(
                        TracingEntry.CTX_TRANSACTION_ID.getKey(),
                        event.getTransactionId() != null
                                ? event.getTransactionId()
                                : TracingEntry.CTX_TRANSACTION_ID.getDefaultValue()
                )
                .put(
                        TracingEntry.CTX_EVENT_CODE.getKey(),
                        event.getEventCode() != null
                                ? event.getEventCode()
                                : TracingEntry.CTX_EVENT_CODE.getDefaultValue()
                )
                .put(
                        TracingEntry.CTX_EVENT_ID.getKey(),
                        event.getId() != null
                                ? event.getId()
                                : TracingEntry.CTX_EVENT_ID.getDefaultValue()
                )
                .put(TracingEntry.EVENT_ACTION.getKey(), "PROCESS_CDC_EVENT");
    }
}
