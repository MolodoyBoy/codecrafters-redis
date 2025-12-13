package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.SimpleStringData;

import static com.my.redis.Command.*;

public class PingCommandExecutor implements CommandExecutor {

    @Override
    public Command supportedCommand() {
        return PING;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        if (commandArgs.args() != null && commandArgs.args().length != 0) {
            throw new IllegalArgumentException("PING command does not accept arguments!");
        }

        return new SimpleStringData("PONG").encode();
    }
}