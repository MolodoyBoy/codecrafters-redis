package com.my.redis.executor;

import com.my.redis.Command;

public interface CommandExecutor {

    Command supportedCommand();

    String execute(CommandArgs commandArgs);
}