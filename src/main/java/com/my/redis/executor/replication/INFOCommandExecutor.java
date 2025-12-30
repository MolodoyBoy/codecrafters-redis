package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

public class INFOCommandExecutor implements CommandExecutor {

    @Override
    public Command supportedCommand() {
        return Command.INFO;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        return new BulkStringData("role:master").encode();
    }
}
