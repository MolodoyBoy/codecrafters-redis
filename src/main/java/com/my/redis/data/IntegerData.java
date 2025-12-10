package com.my.redis.data;

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
}