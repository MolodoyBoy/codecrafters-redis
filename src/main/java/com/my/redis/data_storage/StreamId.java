package com.my.redis.data_storage;

import com.my.redis.exception.ValidationException;

import java.util.Comparator;

public record StreamId(long timestampMillis, long sequence) implements Comparable<StreamId>{

    public StreamId {
        if (timestampMillis <= 0 && sequence <= 0) {
            throw new ValidationException("ERR The ID specified in XADD must be greater than 0-0");
        }
    }
    @Override
    public String toString() {
        return timestampMillis + "-" + sequence;
    }

    @Override
    public int compareTo(StreamId o) {
        return Comparator.comparingLong(StreamId::timestampMillis)
            .thenComparingLong(StreamId::sequence)
            .compare(this, o);
    }
}