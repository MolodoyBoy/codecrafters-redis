package com.my.redis.parser;

import java.util.Arrays;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum ARGUMENT {

    PORT("--port"),
    REPLICAOF("--replicaof");

    private final String value;

    ARGUMENT(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    private static final Map<String, ARGUMENT> VALUE_MAP = Arrays.stream(ARGUMENT.values())
        .collect(toMap(ARGUMENT::value, identity()));

    public static ARGUMENT fromValue(String value) {
        return VALUE_MAP.get(value);
    }
}
