package com.my.redis.data_storage;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

public class KeySpaceStorage {

    private static final String NONE_TYPE = "none";
    private final ConcurrentMap<String, StorageValue> cache;

    public KeySpaceStorage() {
        this.cache = new ConcurrentHashMap<>();
    }

    public String getType(String key) {
        StorageValue storageValue = cache.get(key);
        return storageValue != null ? storageValue.type() : NONE_TYPE;
    }

    public <T extends StorageValue> T get(String key, Class<T> clazz) {
        StorageValue storageValue = cache.get(key);
        if (storageValue == null) {
            return null;
        }

        try {
            return clazz.cast(storageValue);
        } catch (ClassCastException e) {
            throw new IllegalStateException("Expected " + clazz.getSimpleName() + " but got " + storageValue.type());
        }
    }

    public void put(String key, StorageValue storageValue) {
        cache.put(key, storageValue);
    }

    public <T extends StorageValue> T remove(String key, Class<T> clazz) {
        StorageValue storageValue = cache.remove(key);
        if (storageValue == null) {
            return null;
        }

        try {
            return clazz.cast(storageValue);
        } catch (ClassCastException e) {
            throw new IllegalStateException("Expected " + clazz.getSimpleName() + " but got " + storageValue.type());
        }
    }

    public void remove(String key, StorageValue storageValue) {
        cache.remove(key, storageValue);
    }

    public <T extends StorageValue> T computeIfAbsent(String key, StorageValue defaultValue, Class<T> clazz) {
        StorageValue storageValue = cache.computeIfAbsent(key, k -> defaultValue);

        try {
            return clazz.cast(storageValue);
        } catch (ClassCastException e) {
            throw new IllegalStateException("Expected " + clazz.getSimpleName() + " but got " + storageValue.type());
        }
    }
}