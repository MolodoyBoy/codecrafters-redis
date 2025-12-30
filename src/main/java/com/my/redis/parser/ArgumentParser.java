package com.my.redis.parser;

import java.util.EnumMap;
import java.util.Map;

public class ArgumentParser {

    private final Map<ARGUMENT, Object> arguments;

    public ArgumentParser(String[] args) {
        this.arguments = parseArgs(args);
    }

    public int parsePortArg(int defaultPort) {
        Object rawValue = arguments.get(ARGUMENT.PORT);
        if (rawValue == null) {
            return defaultPort;
        }

        if (rawValue instanceof String s) {
            return parsePortValue(s);
        }

        throw new IllegalStateException("Unexpected value type for PORT: " + rawValue.getClass().getName());
    }

    private static int parsePortValue(String raw) {
        try {
            int port = Integer.parseInt(raw);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port out of range: " + port);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port: " + raw, e);
        }
    }

    public String parseReplicaOfArg() {
        Object rawValue = arguments.get(ARGUMENT.REPLICAOF);
        if (rawValue == null) {
            return null;
        }

        if (rawValue instanceof String s) {
            return s;
        }

        throw new IllegalStateException("Unexpected value type for REPLICAOF: " + rawValue.getClass().getName());
    }

    /**
     * Algorithm:
     * - Iterate through args
     * - If current token is an ARGUMENT (checked via ARGUMENT.valueOf(token)), then
     *   collect following tokens as options until the next token is an ARGUMENT.
     * - For this stage, assume each ARGUMENT has only one option.
     */
    private static Map<ARGUMENT, Object> parseArgs(String[] args) {
        if (args == null || args.length == 0) {
            return Map.of();
        }

        EnumMap<ARGUMENT, Object> result = new EnumMap<>(ARGUMENT.class);

        ARGUMENT currentArg = null;
        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            if (token == null || token.isBlank()) {
                continue;
            }

            ARGUMENT maybeArg;
            try {
                maybeArg = ARGUMENT.fromValue(token);
            } catch (IllegalArgumentException ignored) {
                maybeArg = null;
            }

            if (maybeArg != null) {
                currentArg = maybeArg;
                continue;
            }

            result.put(currentArg, token);

            // Since one option is expected, reset.
            currentArg = null;
        }

        return result;
    }
}
