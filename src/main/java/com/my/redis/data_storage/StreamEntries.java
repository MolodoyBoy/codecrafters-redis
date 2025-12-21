package com.my.redis.data_storage;

import java.util.LinkedList;
import java.util.List;

public record StreamEntries(List<StreamKeyValuePair> entries) {

    public static StreamEntries initStreamEntry() {
        return new StreamEntries(new LinkedList<>());
    }

    public void addAll(List<StreamKeyValuePair> keyValuePairs) {
        entries.addAll(keyValuePairs);
    }
}