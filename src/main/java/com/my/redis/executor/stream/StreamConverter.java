package com.my.redis.executor.stream;

import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data_storage.stream.StreamEntries;
import com.my.redis.data_storage.stream.StreamId;

import java.util.Map;
import java.util.NavigableMap;

public class StreamConverter {

    public StreamId convertStreamId(String startId, long sequence, boolean inclusive) {
        if (startId.equals("$")) {
            return new StreamId(null, null, true);
        }

        if (startId.equals("-") || startId.equals("+")) {
            return null;
        }

        String[] split = startId.split("-");
        if (split.length == 1) {
            return new StreamId(Long.parseLong(split[0]), sequence, inclusive);
        } else {
            return new StreamId(Long.parseLong(split[0]), Long.parseLong(split[1]), inclusive);
        }
    }

    public ArrayData convertResult(Map<String, NavigableMap<StreamId, StreamEntries>> result) {
        if (result == null || result.isEmpty()) {
            return new ArrayData(null);
        }

        ArrayData arrayData = new ArrayData(result.size());
        result.forEach((key, streamMap) -> {
            ArrayData keyValueArray = new ArrayData(2);
            keyValueArray.addData(new BulkStringData(key));
            keyValueArray.addData(convertResult(streamMap));
            arrayData.addData(keyValueArray);
        });

        return arrayData;
    }

    public ArrayData convertResult(NavigableMap<StreamId, StreamEntries> result) {
        if (result == null || result.isEmpty()) {
            return new ArrayData(null);
        }

        ArrayData arrayData = new ArrayData(result.size());
        result.forEach((streamId, streamEntries) -> {
            ArrayData keyValueArray = new ArrayData(2);
            keyValueArray.addData(new BulkStringData(streamId.toString()));

            ArrayData entriesData = new ArrayData(streamEntries.entries().size() * 2);
            streamEntries.entries()
                .forEach(entry -> {
                    entriesData.addData(new BulkStringData(entry.key()));
                    entriesData.addData(new BulkStringData(entry.value()));
                });

            keyValueArray.addData(entriesData);
            arrayData.addData(keyValueArray);
        });

        return arrayData;
    }
}
