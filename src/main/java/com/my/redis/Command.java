package com.my.redis;

import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum Command {

    PING, ECHO;

    private final String command;

    Command() {
        this.command = this.name();
    }

    public String getCommand() {
        return command;
    }

    private static final Map<String, Command> COMMAND_MAP = stream(Command.values())
            .collect(toMap(Command::getCommand, identity()));

    public static Command parseCommand(String commandStr) {
        return COMMAND_MAP.get(commandStr.toUpperCase());
    }
}
