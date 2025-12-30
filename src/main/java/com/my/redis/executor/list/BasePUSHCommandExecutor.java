package com.my.redis.executor.list;

import com.my.redis.data.Data;
import com.my.redis.data.IntegerData;
import com.my.redis.data.StringData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class BasePUSHCommandExecutor implements CommandExecutor {

    protected abstract Function<Input, Integer> getPushFunction();

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] data = commandArgs.args();
        if (data == null || data.length < 2) {
            throw new IllegalArgumentException(String.format("%s command requires at least 2 arguments!", supportedCommand()));
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

        Integer size = getPushFunction().apply(new Input(listKey, values, commandArgs.inputData()));

        return new IntegerData(size).encode();
    }

    protected record Input(String key, List<String> values, String query) {}
}
