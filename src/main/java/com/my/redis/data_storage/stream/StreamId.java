package com.my.redis.data_storage.stream;

import com.my.redis.exception.ValidationException;

import java.util.Comparator;

public record StreamId(Long timestampMillis, Long sequence, boolean isInclusive) implements Comparable<StreamId> {

    public StreamId {
        if (timestampMillis != null
            && sequence != null
            && isInclusive
            && timestampMillis <= 0
            && sequence <= 0) {
            throw new ValidationException("ERR The ID specified in XADD must be greater than 0-0");
        }
    }

    public boolean needToBeGenerated() {
        return timestampMillis == null || sequence == null;
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