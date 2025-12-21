package com.my.redis.data_storage.key_space;

import com.my.redis.exception.ValidationException;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

public class KeySpaceStorage {

    private static final String NONE_TYPE = "none";
    private final ConcurrentMap<String, Storage> cache;

    public KeySpaceStorage() {
        this.cache = new ConcurrentHashMap<>();
    }

    public String getType(String key) {
        Storage storage = cache.get(key);
        return storage != null ? storage.type() : NONE_TYPE;
    }

    public <T extends Storage> T get(String key, Class<T> clazz) {
        Storage storage = cache.get(key);
        if (storage == null) {
            return null;
        }

        try {
            return clazz.cast(storage);
        } catch (ClassCastException e) {
            throw new ValidationException("Expected " + clazz.getSimpleName() + " but got " + storage.type());
        }
    }

    public void put(String key, Storage storage) {
        cache.put(key, storage);
    }

    public void remove(String key, Storage storage) {
        cache.remove(key, storage);
    }

    public <T extends Storage> T computeIfAbsent(String key, Storage defaultValue, Class<T> clazz) {
        Storage storage = cache.computeIfAbsent(key, k -> defaultValue);

        try {
            return clazz.cast(storage);
        } catch (ClassCastException e) {
            throw new ValidationException("Expected " + clazz.getSimpleName() + " but got " + storage.type());
        }
    }
}