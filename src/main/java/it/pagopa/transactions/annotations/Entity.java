package it.pagopa.transactions.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * <p>An annotation for entities, as in the Domain Driven Design (DDD) sense.
 * Entities are composites made of value objects.
 * Their primary feature is that they have an identity and can mutate over time.</p>
 * <p>Entities can be distinguished from one another by their identifier.</p>
 */
@Documented
@Target(TYPE)
public @interface Entity {}
