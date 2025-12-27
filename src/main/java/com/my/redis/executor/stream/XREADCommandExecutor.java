package com.my.redis.executor.stream;

import com.my.redis.Command;
import com.my.redis.Option;
import com.my.redis.Utils;
import com.my.redis.data.Data;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.data_storage.stream.StreamId;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.time.Duration;
import java.util.*;

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

        int index = 0;
        String arg1 = args[index++].getStringValue();
        Option option = parseOption(arg1);
        if (option == null) {
            throw new IllegalArgumentException("Unsupported option: " + arg1);
        }

        Duration duration = null;
        if (option == BLOCK) {
            String arg2 = args[index++].getStringValue();
            long millis = Utils.parseLong(arg2);
            duration = Duration.ofMillis(millis);

            String arg3 = args[index++].getStringValue();
            option = parseOption(arg3);
        }

        List<String> streamKeys = new LinkedList<>();
        if (option == STREAMS) {
            while (index < args.length && !Utils.isStreamId(args[index].getStringValue())) {
                streamKeys.add(args[index++].getStringValue());
            }
        }

        List<StreamId> streamIds = new LinkedList<>();
        while (index < args.length) {
            streamIds.add(streamConverter.convertStreamId(args[index++].getStringValue(), 0, false));
        }

        Iterator<String> keyIterator = streamKeys.iterator();
        Iterator<StreamId> idIterator = streamIds.iterator();

        Map<String, StreamId> ids = new LinkedHashMap<>();
        while (keyIterator.hasNext() || idIterator.hasNext()) {
            String streamKey = keyIterator.next();
            StreamId streamId = idIterator.next();
            if (streamKey == null || streamId == null) {
                throw new IllegalArgumentException("Mismatched stream keys and IDs");
            }

            ids.put(streamKey, streamId);
        }

        var result = cache.getInRange(ids, duration);
        return streamConverter.convertResult(result).encode();
    }
}
