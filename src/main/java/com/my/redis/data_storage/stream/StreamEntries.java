package com.my.redis.data_storage.stream;

import java.util.LinkedList;
import java.util.List;

public record StreamEntries(List<StreamEntry> entries) {

    public static StreamEntries initStreamEntry() {
        return new StreamEntries(new LinkedList<>());
    }

    public void addAll(List<StreamEntry> keyValuePairs) {
        entries.addAll(keyValuePairs);
    }
}