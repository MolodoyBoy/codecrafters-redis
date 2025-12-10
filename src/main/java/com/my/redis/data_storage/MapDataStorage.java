package com.my.redis.data_storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MapDataStorage {

    private final Map<String, String> cache = new ConcurrentHashMap<>();

    public String get(String key) {
        return cache.get(key);
    }

    public void put(String key, String value) {
        cache.put(key, value);
    }
}