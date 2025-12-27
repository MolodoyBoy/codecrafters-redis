package com.my.redis.executor.stream;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.data_storage.stream.StreamId;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class XRANGECommandExecutor implements CommandExecutor {

    private final StreamDataStorage cache;
    private final StreamConverter streamConverter;

    public XRANGECommandExecutor(StreamDataStorage cache) {
        this.cache = cache;
        this.streamConverter = new StreamConverter();
    }

    @Override
    public Command supportedCommand() {
        return XRANGE;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException("XRANGE command requires exactly three arguments");
        }

        String key = args[0].getStringValue();

        String startId = args[1].getStringValue();
        String endId = args[2].getStringValue();

        StreamId startStreamId = streamConverter.convertStreamId(startId, 0, true);
        StreamId endStreamId = streamConverter.convertStreamId(endId, Long.MAX_VALUE, true);

        var result = cache.getInRange(key, startStreamId, endStreamId);

        return streamConverter.convertResult(result).encode();
    }
}
