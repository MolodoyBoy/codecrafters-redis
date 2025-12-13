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

    public synchronized int length(String listKey) {
        List<String> list = cache.get(listKey);
        return list == null ? 0 : list.size();
    }
    
    public synchronized int addToTail(String listKey, List<String> values) {
        List<String> list = cache.computeIfAbsent(listKey, key -> new LinkedList<>());

        list.addAll(values);

        return list.size();
    }

    public synchronized int addToHead(String listKey, List<String> values) {
        List<String> list = cache.computeIfAbsent(listKey, key -> new LinkedList<>());

        for (String value : values) {
            list.addFirst(value);
        }

        return list.size();
    }

    public synchronized List<String> get(String listKey, int start, int stop) {
        List<String> list = cache.get(listKey);
        if (list == null || list.isEmpty()) {
            return List.of();
        }

        int size = list.size();

        // Convert negative indices to positive
        // In Redis, -1 means the last element, -2 means second to last, etc.
        if (start < 0) {
            start = size + start;
        }

        if (stop < 0) {
            stop = size + stop;
        }

        if (start < 0) {
            start = 0;
        }

        if (start >= size) {
            return List.of();
        }

        if (stop >= size) {
            stop = size - 1;
        }

        if (stop < 0) {
            return List.of();
        }

        if (start > stop) {
            return List.of();
        }

        return list.subList(start, stop + 1);
    }
}