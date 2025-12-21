package com.my.redis.data;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;

public class SimpleError implements Data {

    private final String message;

    public SimpleError(String message) {
        this.message = message;
    }

    @Override
    public DataType getType() {
        return ERROR;
    }

    @Override
    public String encode() {
        return Character.toString(ERROR.getValue()) + message + CRLF;
    }
}
