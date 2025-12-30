package com.my.redis.parser;

public class ArgumentParser {

    public int parsePortArg(String[] args, int defaultPort) {
        if (args == null || args.length == 0) {
            return defaultPort;
        }

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("--port".equals(a)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value after --port");
                }
                return parsePortValue(args[i + 1]);
            }

            if (a != null && a.startsWith("--port=")) {
                return parsePortValue(a.substring("--port=".length()));
            }
        }

        return defaultPort;
    }

    private int parsePortValue(String raw) {
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
}
