package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.IntegerData;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.ListDataStorage;

import java.util.LinkedList;
import java.util.List;

import static com.my.redis.Command.RPUSH;

public class RPUSHCommandExecutor implements CommandExecutor {

    private final ListDataStorage cache;

    public RPUSHCommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return RPUSH;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] data = commandArgs.args();
        if (data == null || data.length < 2) {
            throw new IllegalArgumentException("RPUSH command requires at least 2 arguments!");
        }

        String listKey = null;

        List<String> values = new LinkedList<>();
        for (Data d : data) {
            if (d instanceof StringData stringData) {
                if (listKey == null) {
                    listKey = stringData.getValue();
                } else {
                    values.add(stringData.getValue());
                }
            }
        }

        int size = cache.add(listKey, values);

        return new IntegerData(size).encode();
    }
}
