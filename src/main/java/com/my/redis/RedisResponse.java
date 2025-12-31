package com.my.redis;

public record RedisResponse(Command command, String outputData) {}