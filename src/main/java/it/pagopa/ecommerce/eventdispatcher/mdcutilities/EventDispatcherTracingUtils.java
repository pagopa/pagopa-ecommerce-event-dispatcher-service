package it.pagopa.ecommerce.eventdispatcher.mdcutilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.MDC;
import reactor.util.context.Context;

/**
 * Utility class with helper methods to enrich Reactor Context for CDC event
 * processing.
 */
public class EventDispatcherTracingUtils {

    private static final String CTX_DETAILS_KEY = "ctx.details";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private EventDispatcherTracingUtils() {
    }

    /** Tracing keys copied from Reactor Context to MDC. */
    public enum TracingEntry {
        CTX_TRANSACTION_ID("ctx.transaction.id", "{transactionId-not-found}", true),
        CTX_EVENT_CODE("ctx.event.code", "{eventCode-not-found}", true),
        CTX_EVENT_ID("ctx.event.id", "{eventId-not-found}", true),
        EVENT_ACTION("event.action", "{eventAction-not-found}", true),
        EVENT_OUTCOME("event.outcome", "{eventOutcome-not-found}", false),
        DEPENDENCY("dependency", "{dependency-not-found}", false),
        ERROR_TYPE("error.type", "{errorType-not-found}", false),
        ERROR_MESSAGE("error.message", "{errorMessage-not-found}", false);

        private final String key;
        private final String defaultValue;
        private final boolean contextBound;

        TracingEntry(
                String key,
                String defaultValue,
                boolean contextBound
        ) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.contextBound = contextBound;
        }

        public String getKey() {
            return key;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public boolean isContextBound() {
            return contextBound;
        }
    }

    /**
     * Enrich Reactor Context with generic event metadata used by MDC/logging hooks.
     */
    public static Context enrichContextForDispatcherEvent(
                                                          String transactionId,
                                                          String eventCode,
                                                          String eventId,
                                                          Context reactorContext
    ) {
        return reactorContext
                .put(
                        TracingEntry.CTX_TRANSACTION_ID.getKey(),
                        transactionId != null
                                ? transactionId
                                : TracingEntry.CTX_TRANSACTION_ID.getDefaultValue()
                )
                .put(
                        TracingEntry.CTX_EVENT_CODE.getKey(),
                        eventCode != null
                                ? eventCode
                                : TracingEntry.CTX_EVENT_CODE.getDefaultValue()
                )
                .put(
                        TracingEntry.CTX_EVENT_ID.getKey(),
                        eventId != null
                                ? eventId
                                : TracingEntry.CTX_EVENT_ID.getDefaultValue()
                )
                .put(TracingEntry.EVENT_ACTION.getKey(), "PROCESS_DISPATCHER_EVENT");
    }

    /**
     * Executes a block with error attributes ({@code error.type} and
     * {@code error.message}) and an arbitrary map of top-level attributes
     * temporarily stored in MDC.
     *
     * <p>
     * Error attributes are extracted from the provided {@link Throwable}. Top-level
     * attributes are passed to MDC cleanup logic where string conversion is
     * handled. All keys are guaranteed to be removed after block execution.
     *
     * @param error      the exception to extract type and message from (can be
     *                   null)
     * @param attributes map of top-level MDC key-value attributes (can be null)
     * @param block      code to execute while attributes are available in MDC
     */
    public static void withErrorMdc(
                                    Throwable error,
                                    Map<String, ?> attributes,
                                    Runnable block
    ) {
        Map<String, Object> mdcMap = new HashMap<>();

        mdcMap.put(
                TracingEntry.ERROR_TYPE.getKey(),
                error != null
                        ? error.getClass().getName()
                        : TracingEntry.ERROR_TYPE.getDefaultValue()
        );
        mdcMap.put(
                TracingEntry.ERROR_MESSAGE.getKey(),
                error != null && error.getMessage() != null
                        ? error.getMessage()
                        : TracingEntry.ERROR_MESSAGE.getDefaultValue()
        );

        if (attributes != null) {
            attributes.forEach(
                    (
                     key,
                     value
                    ) -> {
                        if (key != null && value != null) {
                            mdcMap.put(key, value);
                        }
                    }
            );
        }

        insertIntoMdcAndCleanup(mdcMap, block);
    }

    /**
     * Executes a block with structured error details temporarily inserted in MDC.
     *
     * <p>
     * The method adds {@code error.type} and {@code error.message} keys, executes
     * the provided block, and always removes those keys afterward.
     *
     * @param error error instance used to populate MDC metadata
     * @param block code to execute while error metadata is available in MDC
     */
    public static void withErrorMdc(
                                    Throwable error,
                                    Runnable block
    ) {
        withErrorMdc(error, null, block);
    }

    /**
     * Executes a block with {@code ctx.details} temporarily stored in MDC as a JSON
     * string.
     *
     * <p>
     * The input map is serialized to raw JSON and stored under key
     * {@code ctx.details}. If serialization fails, an empty JSON object
     * ({@code {}}) is used as fallback. The key is always removed after block
     * execution.
     *
     * @param details map of detail values to serialize under {@code ctx.details}
     * @param block   code to execute while {@code ctx.details} is available in MDC
     */
    public static void withContextDetailsMdc(
                                             Map<String, ?> details,
                                             Runnable block
    ) {
        withContextDetailsMdc(details, null, block);
    }

    /**
     * Executes a block with {@code ctx.details} temporarily stored in MDC as a JSON
     * string.
     *
     * <p>
     * The input map is serialized to raw JSON and stored under key
     * {@code ctx.details}. If serialization fails, an empty JSON object
     * ({@code {}}) is used as fallback. The key is always removed after block
     * execution.
     *
     * @param details map of detail values to serialize under {@code ctx.details}
     * @param block   code to execute while {@code ctx.details} is available in MDC
     */
    public static void withContextDetailsMdc(
                                             Map<String, ?> details,
                                             Map<String, ?> attributes,
                                             Runnable block
    ) {
        Map<String, Object> mdcMap = new HashMap<>();

        String rawDetails = "{}";
        if (details != null) {
            try {
                rawDetails = OBJECT_MAPPER.writeValueAsString(details);
            } catch (JsonProcessingException ignored) {
                rawDetails = "{}";
            }
        }
        mdcMap.put(CTX_DETAILS_KEY, rawDetails);

        if (attributes != null) {
            attributes.forEach(
                    (
                     k,
                     v
                    ) -> {
                        if (k != null && v != null) {
                            mdcMap.put(k, v);
                        }
                    }
            );
        }

        insertIntoMdcAndCleanup(mdcMap, block);
    }

    /**
     * Inserts the provided entries into MDC, executes the given block, and always
     * removes the inserted keys afterward.
     *
     * <p>
     * This method guarantees MDC cleanup through a {@code finally} block, so
     * temporary values do not leak across log statements or threads.
     *
     * @param entries key/value pairs to temporarily add to MDC
     * @param block   code to execute while MDC entries are available
     */
    private static void insertIntoMdcAndCleanup(
                                                Map<String, ?> entries,
                                                Runnable block
    ) {
        List<String> detailKeys = new ArrayList<>();

        try {
            if (entries != null) {
                entries.forEach(
                        (
                         key,
                         value
                        ) -> {
                            if (key != null && value != null) {
                                MDC.put(key, value.toString());
                                detailKeys.add(key);
                            }
                        }
                );
            }
            block.run();
        } finally {
            detailKeys.forEach(MDC::remove);
        }
    }
}
