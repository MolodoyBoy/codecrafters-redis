package com.my.redis.executor.transaction;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.IntegerData;
import com.my.redis.data.SimpleErrorData;
import com.my.redis.data_storage.map.MapDataStorage;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class INCRCommandExecutor implements CommandExecutor {

    private final MapDataStorage dataStorage;

    public INCRCommandExecutor(MapDataStorage dataStorage) {
        this.dataStorage = dataStorage;
    }

    @Override
    public Command supportedCommand() {
        return INCR;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("INCR command requires exactly one arguments!");
        }

        String key = args[0].getStringValue();

        int result = dataStorage.increment(key, commandArgs.inputData());
        if (result == -1) {
            return new SimpleErrorData("ERR value is not an integer or out of range").encode();
        }

        return new IntegerData(result).encode();
    }
}
