package com.my.redis.data_storage;

import java.util.HashMap;
import java.util.Map;

public class MapDataStorage {

    private final Map<String, String> cache = new HashMap<>();

    public synchronized String get(String key) {
        return cache.get(key);
    }

    public synchronized void put(String key, String value) {
        cache.put(key, value);
    }
}