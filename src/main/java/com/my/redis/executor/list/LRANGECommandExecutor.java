package com.my.redis.executor.list;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data_storage.list.ListDataStorage;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;
import static com.my.redis.Utils.parseInt;
import static com.my.redis.Utils.toStringData;

public class LRANGECommandExecutor implements CommandExecutor {

    private final ListDataStorage cache;

    public LRANGECommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return LRANGE;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args.length != 3) {
            throw new IllegalArgumentException("LRANGE command requires exactly 3 arguments");
        }

        String listKey = toStringData(args[0]).getValue();
        int stop = parseInt(toStringData(args[2]));
        int start = parseInt(toStringData(args[1]));

        var values = cache.get(listKey, start, stop);
        ArrayData arrayData = new ArrayData(values.size());

        for (String value : values) {
            arrayData.addData(new BulkStringData(value));
        }

        return arrayData.encode();
    }
}
