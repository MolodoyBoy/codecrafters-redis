package com.my.redis.executor.common;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.StringData;
import com.my.redis.executor.base.CommandExecutor;
import com.my.redis.executor.args.CommandArgs;

import static com.my.redis.Command.*;

public class EchoCommandExecutor implements CommandExecutor {

    @Override
    public Command supportedCommand() {
        return ECHO;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();

        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("ECHO command requires one argument!");
        }

        if (args[0] instanceof StringData stringData) {
            return stringData.encode();
        }

        throw new IllegalArgumentException("ECHO argument must be a string!");
    }
}