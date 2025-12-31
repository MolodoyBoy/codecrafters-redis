package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.IntegerData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class WAITCommandExecutor implements CommandExecutor {

    @Override
    public Command supportedCommand() {
        return WAIT;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        return new IntegerData(0).encode();
    }
}