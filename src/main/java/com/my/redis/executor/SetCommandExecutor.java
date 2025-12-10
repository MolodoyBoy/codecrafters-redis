package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.MapDataStorage;

import static com.my.redis.Command.*;

public class SetCommandExecutor implements CommandExecutor {

    private final MapDataStorage cache;

    public SetCommandExecutor(MapDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return SET;
    }

    @Override
    public synchronized String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();

        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("SET command requires two arguments!");
        }

        if (args[0] instanceof StringData key && args[1] instanceof StringData value) {
            cache.put(key.getValue(), value.getValue());
            return new SimpleStringData("OK").decorate();
        }

        throw new IllegalArgumentException("SET arguments must be strings!");
    }
}
