package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.Option;
import com.my.redis.data.Data;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.MapDataStorage;
import com.my.redis.data_storage.ValueData;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.my.redis.Command.*;
import static com.my.redis.Option.*;
import static java.lang.System.currentTimeMillis;

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

        if (args == null || args.length < 2) {
            throw new IllegalArgumentException("SET command requires at least 2 arguments!");
        }

        String key;
        String value;

        if (args[0] instanceof StringData keyData) {
            key = keyData.getValue();
        } else {
            throw new IllegalArgumentException("SET key must be a string!");
        }

        if (args[1] instanceof StringData valueData) {
            value = valueData.getValue();
        } else {
            throw new IllegalArgumentException("SET value must be a string!");
        }

        Long expirationTime = getExpirationTime(args);
        cache.put(key, new ValueData(value, expirationTime));

        return new SimpleStringData("OK").encode();
    }

    private Long getExpirationTime(Data[] args) {
        if (args.length == 2) {
            return null;
        }

        if (args.length != 4) {
            throw new IllegalArgumentException("SET command requires 4 or 2 arguments!");
        }

        Option option = findOption(args[2]);
        if (!supportedCommand().supportOption(option)) {
            throw new IllegalArgumentException(String.format("Not supported option %s for command SET!", option));
        }

        Data timeArg = args[3];

        if (timeArg instanceof StringData timeData) {
            String timeStr = timeData.getValue();

            int time = Integer.parseInt(timeStr);

            Duration duration = Duration.of(time, ChronoUnit.MILLIS);

            long millis = duration.toMillis();
            return currentTimeMillis() + millis;
        }

        throw new IllegalArgumentException("Invalid time argument!");
    }

    private Option findOption(Data shouldBeOption) {
        if (shouldBeOption instanceof StringData stringData) {
            Option option = parseOption(stringData.getValue());

            if (option != null) {
                return option;
            }
        }

        throw new IllegalArgumentException("Invalid option!");
    }
}
