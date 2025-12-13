package com.my.redis.data_storage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ListDataStorage {

    private final Map<String, List<String>> cache;

    public ListDataStorage() {
        this.cache = new HashMap<>();
    }

    public synchronized int add(String listKey, List<String> values) {
        List<String> list = cache.computeIfAbsent(listKey, key -> new LinkedList<>());

        list.addAll(values);

        return list.size();
    }
}