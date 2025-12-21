package com.my.redis.executor.stream;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data.SimpleError;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.data_storage.stream.StreamId;
import com.my.redis.data_storage.stream.StreamEntry;
import com.my.redis.exception.ValidationException;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.util.ArrayList;
import java.util.List;

import static com.my.redis.Command.*;
import static com.my.redis.Utils.*;

public class XADDCommandExecutor implements CommandExecutor {

    private final StreamDataStorage cache;

    public XADDCommandExecutor(StreamDataStorage cache) {
        this.cache = cache;
    }

    @Override
    public Command supportedCommand() {
        return XADD;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("XADD command requires at least one argument!");
        }

        try {
            String streamKey = parseString(args[0]);
            String streamIdString = parseString(args[1]);

            StreamId streamId = getStreamId(streamIdString);

            List<StreamEntry> keyPairs = new ArrayList<>();
            for (int i = 2; i < args.length; i += 2) {
                String key = parseString(args[i]);
                String value = parseString(args[i + 1]);

                keyPairs.add(new StreamEntry(key, value));
            }

            StreamId result = cache.addEntries(streamKey, streamId, keyPairs);

            return new BulkStringData(result.toString()).encode();
        } catch (ValidationException e) {
            return new SimpleError(e.getMessage()).encode();
        }
    }

    private StreamId getStreamId(String streamIdString) {
        if (streamIdString.equals("*")) {
            return new StreamId(null, null);
        }

        String[] parts = streamIdString.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid stream ID format!");
        }

        long timestamp = Long.parseLong(parts[0]);

        String sequenceFragment = parts[1];
        long sequence;
        if (sequenceFragment.equals("*")) {
            return new StreamId(timestamp, null);
        } else {
            sequence = Long.parseLong(sequenceFragment);
        }

        return new StreamId(timestamp, sequence);
    }
}
