package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data_storage.ListDataStorage;

import static com.my.redis.Command.*;
import static com.my.redis.Utils.parseInt;
import static com.my.redis.Utils.toStringData;

public class LPOPCommandExecutor implements CommandExecutor {

    private final ListDataStorage cache;

    public LPOPCommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return LPOP;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length > 2 || args.length == 0) {
            throw new IllegalArgumentException("LPOP command requires one or two argument!");
        }

        int count = 1;
        String listKey = toStringData(args[0]).getValue();
        if (args.length == 2) {
            count = parseInt(toStringData(args[1]));
        }

        String removed = cache.remove(listKey, 1);

        return new BulkStringData(removed).encode();
    }
}
