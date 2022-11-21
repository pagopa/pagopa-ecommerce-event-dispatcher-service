package it.pagopa.transactions.domain;

public interface EventUpdatable<T, E> {
    T apply(E event);
}
