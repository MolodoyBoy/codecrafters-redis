package com.my.redis.data;

import java.util.Map;

import static java.util.Arrays.*;
import static java.util.function.Function.*;
import static java.util.stream.Collectors.*;

public enum DataType {

    NULL('_'),
    ARRAY('*'),
    INTEGER(':'),
    BULK_STRING('$'),
    SIMPLE_STRING('+');

    private final int value;

    DataType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    private static final Map<Integer, DataType> VALUE_MAP = stream(values())
            .collect(toMap(DataType::getValue, identity()));

    public static DataType dataTypeFactory(int value) {
        return VALUE_MAP.get(value);
    }
}