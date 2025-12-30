package com.my.redis;

import java.util.Map;
import java.util.Set;

import static com.my.redis.Option.*;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum Command {

    PING(false),
    ECHO(false),
    SET(true, PX),
    GET(false),
    RPUSH(true),
    LRANGE(false),
    LPUSH(true),
    LLEN(false),
    LPOP(true),
    BLPOP(true),
    TYPE(false),
    XADD(true),
    XRANGE(false),
    XREAD(false, BLOCK, STREAMS),
    INCR(true),
    MULTI(false),
    EXEC(false),
    DISCARD(false),
    INFO(false),
    REPLCONF(false),
    PSYNC(false);

    private final String command;
    private final Set<Option> options;
    private final boolean writeCommand;

    Command(boolean writeCommand, Option... options) {
        this.command = this.name();
        this.options = Set.of(options);
        this.writeCommand = writeCommand;
    }

    public String command() {
        return command;
    }

    public boolean writeCommand() {
        return writeCommand;
    }

    public boolean supportOption(Option option) {
        return options.contains(option);
    }

    private static final Map<String, Command> COMMAND_MAP = stream(Command.values())
            .collect(toMap(Command::command, identity()));

    public static Command parseCommand(String commandStr) {
        return COMMAND_MAP.get(commandStr.toUpperCase());
    }
}
