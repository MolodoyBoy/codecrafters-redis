package com.my.redis.data;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;

public class IntegerData implements Data {

    private final int value;

    public IntegerData(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public DataType getType() {
        return INTEGER;
    }

    @Override
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(Character.toString(getType().getValue()));

        if (value > 0) {
            sb.append(Character.toString(PLUS));
        }

        sb.append(value);
        sb.append(CRLF);

        return sb.toString();
    }

    @Override
    public String getStringValue() {
        return Integer.toString(value);
    }
}