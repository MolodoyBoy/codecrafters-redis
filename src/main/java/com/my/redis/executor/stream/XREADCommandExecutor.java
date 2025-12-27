package com.my.redis.executor.stream;

import com.my.redis.Command;
import com.my.redis.Option;
import com.my.redis.Utils;
import com.my.redis.data.Data;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.data_storage.stream.StreamId;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.util.LinkedList;
import java.util.List;

import static com.my.redis.Command.*;
import static com.my.redis.Option.*;

public class XREADCommandExecutor implements CommandExecutor {

    private final StreamDataStorage cache;
    private final StreamConverter streamConverter;

    public XREADCommandExecutor(StreamDataStorage cache) {
        this.cache = cache;
        this.streamConverter = new StreamConverter();
    }

    @Override
    public Command supportedCommand() {
        return XREAD;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException("XREAD command requires at least three arguments");
        }

        String arg1 = args[0].getStringValue();
        Option option = parseOption(arg1);
        if (option == null) {
            throw new IllegalArgumentException("Unsupported option: " + arg1);
        }

        List<String> streamKeys = new LinkedList<>();

        if (option == STREAMS) {
            int index = 1;
            String key = args[index].getStringValue();
            while (index < args.length && !Utils.isInt(key)) {
                streamKeys.add(key);
                key = args[index++].getStringValue();
            }
        }

        StreamId streamId = streamConverter.convertStreamId(args[args.length - 1].getStringValue(), 0, false);

        var result = cache.getInRange(streamKeys, streamId, null);
        return streamConverter.convertResult(result).encode();
    }
}
