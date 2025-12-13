package com.my.redis.data;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;

public class NullData implements Data {

    @Override
    public DataType getType() {
        return NULL;
    }

    @Override
    public String encode() {
        return Character.toString(getType().getValue()) + CRLF;
    }
}