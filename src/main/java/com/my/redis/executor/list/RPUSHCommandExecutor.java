package com.my.redis.executor.list;

import com.my.redis.Command;
import com.my.redis.data_storage.list.ListDataStorage;

import java.util.List;
import java.util.function.BiFunction;

import static com.my.redis.Command.RPUSH;

public class RPUSHCommandExecutor extends BasePUSHCommandExecutor {

    private final ListDataStorage cache;

    public RPUSHCommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return RPUSH;
    }

    @Override
    protected BiFunction<String, List<String>, Integer> getPushFunction() {
        return cache::addToTail;
    }
}
