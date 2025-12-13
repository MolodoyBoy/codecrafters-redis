package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.MapDataStorage;
import com.my.redis.data_storage.ValueData;

import static com.my.redis.Command.GET;

public class GetCommandExecutor implements CommandExecutor {

    private final MapDataStorage cache;

    public GetCommandExecutor(MapDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return GET;
    }

    @Override
    public synchronized String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();

        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("GET command requires one argument!");
        }

        if (args[0] instanceof StringData key) {
            ValueData valueData = cache.get(key.getValue());

            if (valueData == null) {
                return new BulkStringData(null).decorate();
            }

            return new BulkStringData(valueData.value()).decorate();
        }

        throw new IllegalArgumentException("GET arguments must be strings!");
    }
}