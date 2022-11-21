package it.pagopa.transactions.domain;

import it.pagopa.transactions.annotations.ValueObject;

@ValueObject
public record TransactionDescription(String value) {}
