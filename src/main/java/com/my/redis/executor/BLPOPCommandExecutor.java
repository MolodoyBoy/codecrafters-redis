package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data_storage.ListDataStorage;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.my.redis.Command.*;
import static com.my.redis.Utils.*;
import static java.util.concurrent.TimeUnit.*;

public class BLPOPCommandExecutor implements CommandExecutor {

    private final ListDataStorage cache;

    public BLPOPCommandExecutor(ListDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return BLPOP;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("BLPOP command requires at least two argument!");
        }

        List<String> listKeys = new LinkedList<>();

        int lastIndex = args.length - 1;
        for (int i = 0; i < lastIndex; i++) {
            Data arg = args[i];
            listKeys.add(toStringData(arg).getValue());
        }

        long time;
        double timeoutD = parseDouble(toStringData(args[lastIndex]));
        TimeUnit unit;
        if (timeoutD < 1) {
            unit = MILLISECONDS;
            time = (int) (timeoutD * 1000);
        } else {
            time = (int) timeoutD;
            unit = SECONDS;
        }

        Map.Entry<String, String> element = cache.poll(listKeys, time, unit);
        if (element == null) {
            return new ArrayData(null).encode();
        }

        return new ArrayData(new Data[] {
            new BulkStringData(element.getKey()),
            new BulkStringData(element.getValue())
        }).encode();
    }
}
