package com.my.redis.executor.list;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.IntegerData;
import com.my.redis.data_storage.list.ListDataStorage;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;
import static com.my.redis.Utils.*;

public class LLENCommandExecutor implements CommandExecutor {

    private final ListDataStorage cache;

    public LLENCommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return LLEN;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("LLEN command requires exactly one argument");
        }

        String listKey = toStringData(args[0]).getValue();

        int length = cache.length(listKey);

        return new IntegerData(length).encode();
    }
}
