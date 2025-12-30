package com.my.redis.executor.base;

import com.my.redis.Command;
import com.my.redis.executor.args.CommandArgs;

import java.util.List;

public interface CommandExecutor {

    Command supportedCommand();

    default boolean needTransaction() {
        return true;
    }

    String execute(CommandArgs commandArgs);
}