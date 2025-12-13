package com.my.redis.data;

public interface Data {

    DataType getType();

    String encode();
}