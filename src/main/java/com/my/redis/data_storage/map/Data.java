package com.my.redis.data_storage.map;

import com.my.redis.data_storage.key_space.Storage;

public record Data(String value, Long expireAtMillis) implements Storage {

    @Override
    public String type() {
        return "string";
    }
}
