package com.my.redis.executor.stream;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.data_storage.stream.StreamId;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class XRANGECommandExecutor implements CommandExecutor {

    private final StreamDataStorage cache;

    public XRANGECommandExecutor(StreamDataStorage cache) {
        this.cache = cache;
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

        StreamId startStreamId = getStreamId(startId, 0);
        StreamId endStreamId = getStreamId(endId, Long.MAX_VALUE);

        var result = cache.getInRange(key, startStreamId, endStreamId);

        ArrayData arrayData = new ArrayData(result.size());
        result.forEach((streamId, streamEntries) -> {
           ArrayData keyValueArray = new ArrayData(2);
           keyValueArray.addData(new BulkStringData(streamId.toString()));

           ArrayData entriesData = new ArrayData(streamEntries.entries().size() * 2);
           streamEntries.entries()
                   .forEach(entry -> {;
                       entriesData.addData(new BulkStringData(entry.key()));
                       entriesData.addData(new BulkStringData(entry.value()));
                   });

            keyValueArray.addData(entriesData);
            arrayData.addData(keyValueArray);
        });

        return arrayData.encode();
    }

    private StreamId getStreamId(String startId, long sequence) {
        String[] split = startId.split("-");
        if (split.length == 1) {
            return new StreamId(Long.parseLong(split[0]), sequence);
        } else {
            return new StreamId(Long.parseLong(split[0]), Long.parseLong(split[1]));
        }
    }
}
