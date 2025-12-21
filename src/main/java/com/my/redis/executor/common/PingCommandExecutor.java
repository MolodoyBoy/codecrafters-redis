package com.my.redis.executor.common;

import com.my.redis.Command;
import com.my.redis.data.SimpleStringData;
import com.my.redis.executor.base.CommandExecutor;
import com.my.redis.executor.args.CommandArgs;

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