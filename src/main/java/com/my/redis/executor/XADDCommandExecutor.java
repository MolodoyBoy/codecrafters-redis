package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.Utils;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;
import com.my.redis.data.SimpleError;
import com.my.redis.data_storage.StreamDataStorage;
import com.my.redis.data_storage.StreamId;
import com.my.redis.data_storage.StreamKeyValuePair;
import com.my.redis.exception.ValidationException;

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

            StreamId streamId;
            String[] split = streamIdString.split("-");
            if (split.length != 2) {
                throw new IllegalArgumentException("Invalid stream ID format!");
            } else {
                streamId = new StreamId(Long.parseLong(split[0]), Long.parseLong(split[1]));
            }

            List<StreamKeyValuePair> keyPairs = new ArrayList<>();
            for (int i = 2; i < args.length; i += 2) {
                String key = parseString(args[i]);
                String value = parseString(args[i + 1]);

                keyPairs.add(new StreamKeyValuePair(key, value));
            }

            StreamId result = cache.addEntries(streamKey, streamId, keyPairs);

            return new BulkStringData(result.toString()).encode();
        } catch (ValidationException e) {
            return new SimpleError(e.getMessage()).encode();
        }
    }
}
