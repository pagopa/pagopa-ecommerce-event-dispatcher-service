package it.pagopa.transactions.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * <p>An annotation for aggregate roots, as in the Domain Driven Design (DDD) sense.
 * Aggregate roots are composites of entities and value objects that are responsible for maintaining business logic invariants.</p>
 * <p>Each aggregate root has an identifier (an <code>@AggregateId</code>) which can be used to refer to this aggregate root by other aggregate roots.
 * Entities inside aggregate roots should never be referenced by external aggregate roots.</p>
 */
@Documented
@Target(TYPE)
public @interface AggregateRoot {}
