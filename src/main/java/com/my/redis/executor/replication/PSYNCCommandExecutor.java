package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.data.SimpleStringData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class PSYNCCommandExecutor implements CommandExecutor {

    @Override
    public Command supportedCommand() {
        return PSYNC;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        return new SimpleStringData("OK").encode();
    }
}
