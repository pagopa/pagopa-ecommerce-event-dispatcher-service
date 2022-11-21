package it.pagopa.transactions.domain;

import it.pagopa.transactions.annotations.ValueObject;

import java.util.regex.Pattern;

@ValueObject
public record RptId(String value) {
    private static final Pattern rptIdRegex = Pattern.compile("([a-zA-Z\\d]{29})");

    //RtpId = CF(0,10)+NotID(11,28)
    //NotID = AuxDigit(0)+ApplicationCode(1,2)+CodiceIUV(3,18) || AuxDigit(0)+CodiceIUV(1,18)

    public RptId {
        if (!rptIdRegex.matcher(value).matches()) {
            throw new IllegalArgumentException("Ill-formed RPT id: " + value + ". Doesn't match format: " + rptIdRegex.pattern());
        }

    }

    public String getFiscalCode() {
        return value.substring(0, 11);
    }

    public String getNoticeId() {
        return value.substring(11);
    }

    public String getAuxDigit()  {
        return getNoticeId().substring(0,1);
    }

    public String getApplicationCode()  {
        return ("0").equals(getAuxDigit()) ? getNoticeId().substring(1,3) : null;
    }

    public String getIUV()  {
        return getNoticeId().substring(getApplicationCode() != null ? 3 : 1,18);
    }
}
