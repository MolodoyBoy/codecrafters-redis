package com.my.redis.data;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;

public class SimpleStringData implements StringData {

    private final String value;

    public SimpleStringData(String value) {
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public DataType getType() {
        return SIMPLE_STRING;
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

        String parsedType = Character.toString(getType().getValue());
        return parsedType + value + CRLF;
    }
}