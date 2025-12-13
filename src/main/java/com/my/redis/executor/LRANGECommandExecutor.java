package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.ListDataStorage;

import static com.my.redis.Command.*;

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

        String listKey = getData(args[0]).getValue();
        int stop = parseStringData(getData(args[2]));
        int start = parseStringData(getData(args[1]));

        var values = cache.get(listKey, start, stop);
        ArrayData arrayData = new ArrayData(values.size());

        for (String value : values) {
            arrayData.addData(new BulkStringData(value));
        }

        return arrayData.encode();
    }

    private int parseStringData(StringData stringData) {
        try {
            return Integer.parseInt(stringData.getValue());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected integer value but got: " + stringData.getValue());
        }
    }

    private StringData getData(Data data) {
        if (data instanceof StringData stringData) {
            return stringData;
        }

        throw new IllegalArgumentException("Expected StringData type");
    }
}
