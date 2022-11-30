package it.pagopa.transactions.domain;
import it.pagopa.ecommerce.commons.annotations.ValueObject;
import java.util.UUID;

@ValueObject
public record TransactionId(UUID value) {}
