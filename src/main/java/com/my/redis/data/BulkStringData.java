package com.my.redis.data;

import static com.my.redis.Delimiter.CRLF;
import static com.my.redis.data.DataType.*;

public class BulkStringData implements StringData {

    private final String value;

    public BulkStringData(String value) {
        this.value = value;
    }

    @Override
    public DataType getType() {
        return BULK_STRING;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String encode() {
        String parsedType = Character.toString(getType().getValue());

        if (value == null) {
            return parsedType + -1 + CRLF;
        }

        return parsedType + value.length() + CRLF + value + CRLF;
    }
}