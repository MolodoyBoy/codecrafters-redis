package com.my.redis;

import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum Option {

    PX, STREAMS, BLOCK;

    private final String option;

    Option() {
        this.option = this.name();
    }

    public String option() {
        return option;
    }

    private static final Map<String, Option> OPTION_MAP = stream(Option.values())
            .collect(toMap(Option::option, identity()));

    public static Option parseOption(String optionStr) {
        return OPTION_MAP.get(optionStr.toUpperCase());
    }
}