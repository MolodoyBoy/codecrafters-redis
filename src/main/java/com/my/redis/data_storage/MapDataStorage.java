package com.my.redis.data_storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MapDataStorage {

    private final Map<String, ValueData> cache = new ConcurrentHashMap<>();

    public ValueData get(String key) {
        return cache.get(key);
    }

    public void put(String key, ValueData value) {
        cache.put(key, value);
    }

    public void remove(String key) {
        cache.remove(key);
    }
}