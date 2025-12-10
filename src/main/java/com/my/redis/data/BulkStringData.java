package com.my.redis.data;

import static com.my.redis.Delimiter.CRLF;
import static com.my.redis.data.DataType.*;

public class BulkStringData implements StringData {

    private final String value;

    public BulkStringData(String value) {
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
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
    public String decorate() {
        if (value == null) {
            return null;
        }

        return getType().getValue() + value.length() + CRLF + value + CRLF;
    }
}