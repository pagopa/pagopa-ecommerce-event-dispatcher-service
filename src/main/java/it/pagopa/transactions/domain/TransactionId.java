package it.pagopa.transactions.domain;

import it.pagopa.transactions.annotations.ValueObject;

import java.util.UUID;

@ValueObject
public record TransactionId(UUID value) {}
