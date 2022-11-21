package it.pagopa.transactions.domain;

import it.pagopa.transactions.annotations.ValueObject;
import lombok.Value;
import org.springframework.data.annotation.PersistenceConstructor;

import java.util.regex.Pattern;

@ValueObject
@Value
public class IdempotencyKey {
    private static final Pattern pspFiscalCodeRegex = Pattern.compile("\\d{11}");
    private static final Pattern keyIdentifierRegex = Pattern.compile("[a-zA-Z\\d]{10}");
    String key;

    public IdempotencyKey(String pspFiscalCode, String keyIdentifier) {
        validateComponents(pspFiscalCode, keyIdentifier);

        this.key = pspFiscalCode + "_" + keyIdentifier;
    }

    @PersistenceConstructor
    public IdempotencyKey(String key) {
        String[] matches = key.split("_");

        if (matches.length != 2) {
            throw new IllegalArgumentException("Key doesn't match format `$pspFiscalCode_$keyIdentifier`");
        }

        String pspFiscalCode = matches[0];
        String keyIdentifier = matches[1];

        validateComponents(pspFiscalCode, keyIdentifier);

        this.key = key;
    }

    private static void validateComponents(String pspFiscalCode, String keyIdentifier) {
        if (!pspFiscalCodeRegex.matcher(pspFiscalCode).matches()) {
            throw new IllegalArgumentException("PSP fiscal code doesn't match regex: " + pspFiscalCodeRegex.pattern());
        }

        if (!keyIdentifierRegex.matcher(keyIdentifier).matches()) {
            throw new IllegalArgumentException("Key identifier doesn't match regex: " + keyIdentifierRegex.pattern());
        }
    }
}
