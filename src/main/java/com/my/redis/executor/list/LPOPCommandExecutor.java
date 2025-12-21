package com.my.redis.executor.list;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data_storage.list.ListDataStorage;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.util.List;

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

        String listKey = toStringData(args[0]).getValue();

        if (args.length == 1) {
            List<String> removed = cache.remove(listKey, 1);
            if (removed == null) {
                return new BulkStringData(null).encode();
            }

            return new BulkStringData(removed.getFirst()).encode();
        }

        int count = parseInt(toStringData(args[1]));

        List<String> removed = cache.remove(listKey, count);
        if (removed == null) {
            return new ArrayData(null).encode();
        }

        ArrayData arrayData = new ArrayData(removed.size());
        for (String value : removed) {
            arrayData.addData(new BulkStringData(value));
        }

        return arrayData.encode();
    }
}
