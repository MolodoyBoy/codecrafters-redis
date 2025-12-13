package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data_storage.ListDataStorage;

import java.util.List;
import java.util.function.BiFunction;

import static com.my.redis.Command.*;

public class LPUSHCommandExecutor extends BasePUSHCommandExecutor {

    private final ListDataStorage cache;

    public LPUSHCommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return LPUSH;
    }

    @Override
    protected BiFunction<String, List<String>, Integer> getPushFunction() {
        return cache::addToHead;
    }
}
