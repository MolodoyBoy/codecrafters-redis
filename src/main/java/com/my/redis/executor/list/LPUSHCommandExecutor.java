package com.my.redis.executor.list;

import com.my.redis.Command;
import com.my.redis.data_storage.list.ListDataStorage;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

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
    protected Function<Input, Integer> getPushFunction() {
        return input -> cache.addToHead(input.key(), input.values(), input.query());
    }
}
