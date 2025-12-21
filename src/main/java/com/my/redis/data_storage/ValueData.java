package com.my.redis.data_storage;

public record ValueData(String value, Long expireAtMillis) implements StorageValue {

    @Override
    public String type() {
        return "string";
    }
}
