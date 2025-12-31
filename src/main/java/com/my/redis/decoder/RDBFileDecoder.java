package com.my.redis.decoder;

import java.io.BufferedInputStream;
import java.io.IOException;

import static com.my.redis.data.DataType.*;

public class RDBFileDecoder extends BaseDecoder {

    public RDBFileDecoder(BufferedInputStream in) {
        super(in);
    }

    public byte[] encode() throws IOException {
        int dataType = read();

        if (dataType != BULK_STRING.getValue()) {
            throw new IllegalArgumentException("Invalid RDB file format!");
        }

        int length = encodeLength();

        return readNBytes(length);
    }
}
