package com.my.redis.data;

import static com.my.redis.data.DataType.*;

public class NullData implements Data {

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public DataType getType() {
        return NULL;
    }
}