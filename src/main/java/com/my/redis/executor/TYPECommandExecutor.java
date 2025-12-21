package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data_storage.ListDataStorage;
import com.my.redis.data_storage.MapDataStorage;
import com.my.redis.data_storage.ValueData;

import static com.my.redis.Command.TYPE;
import static com.my.redis.Utils.*;

public class TYPECommandExecutor implements CommandExecutor {

    private final MapDataStorage mapDataStorage;
    private final ListDataStorage listDataStorage;

    public TYPECommandExecutor(MapDataStorage mapDataStorage, ListDataStorage listDataStorage) {
        this.mapDataStorage = mapDataStorage;
        this.listDataStorage = listDataStorage;
    }

    @Override
    public Command supportedCommand() {
        return TYPE;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args.length != 1) {
            throw new IllegalArgumentException("TYPE command requires exactly 1 argument!");
        }

        String key = parseString(args[0]);

        ValueData valueData = mapDataStorage.get(key);
        if (valueData != null) {
            return new SimpleStringData("string").encode();
        } else {
            if (listDataStorage.containsKey(key)) {
                return new SimpleStringData("list").encode();
            }

            return new SimpleStringData("none").encode();
        }
    }
}
