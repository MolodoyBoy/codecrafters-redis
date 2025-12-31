package com.my.redis.parser;

import java.util.Arrays;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum Argument {

    PORT("--port"),
    REPLICAOF("--replicaof");

    private final String value;

    Argument(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    private static final Map<String, Argument> VALUE_MAP = Arrays.stream(Argument.values())
        .collect(toMap(Argument::value, identity()));

    public static Argument fromValue(String value) {
        return VALUE_MAP.get(value);
    }
}
