package com.my.redis.server;

public record RedisResponse(String data, byte[] additional) {
}
