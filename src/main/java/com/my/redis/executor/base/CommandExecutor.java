package com.my.redis.executor.base;

import com.my.redis.Command;
import com.my.redis.executor.args.CommandArgs;

public interface CommandExecutor {

    Command supportedCommand();

    default boolean manageTransaction() {
        return false;
    }

    String execute(CommandArgs commandArgs);
}