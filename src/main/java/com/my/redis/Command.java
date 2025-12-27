package com.my.redis;

import java.util.Map;
import java.util.Set;

import static com.my.redis.Option.*;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum Command {

    PING(), ECHO(), SET(PX), GET(), RPUSH(), LRANGE(), LPUSH(), LLEN(), LPOP(), BLPOP(), TYPE(), XADD(), XRANGE(),
    XREAD(BLOCK, STREAMS);

    private final String command;
    private final Set<Option> options;

    Command(Option... options) {
        this.command = this.name();
        this.options = Set.of(options);
    }

    public String command() {
        return command;
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
