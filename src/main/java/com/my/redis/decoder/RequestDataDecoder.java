package com.my.redis.decoder;

import com.my.redis.data.*;

import java.io.IOException;
import java.io.EOFException;
import java.io.BufferedInputStream;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;
import static java.nio.charset.StandardCharsets.*;

public class RequestDataDecoder extends BaseDecoder {

    public RequestDataDecoder(BufferedInputStream in) {
        super(in);
    }

    public Data encode() throws IOException {
        int firstRead = read();

        return encodeData(firstRead);
    }

    private Data encodeData() throws IOException {
        return encodeData(null);
    }

    private Data encodeData(Integer firstRead) throws IOException {
        int dataTypeValue;
        if (firstRead != null) {
            dataTypeValue = firstRead;
        } else {
            dataTypeValue = read();
        }

        if (dataTypeValue == -1) {
            throw new EOFException("Unexpected end of stream!");
        }

        DataType dataType = dataTypeFactory(dataTypeValue);
        if (dataType == null) {
            throw new IllegalArgumentException("Invalid outputData type!");
        }

        return switch (dataType) {
            case NULL -> new NullData();
            case ARRAY -> encodeArray();
            case BULK_STRING -> encodeBulkString();
            case SIMPLE_STRING -> encodeSimpleString();
            case INTEGER, ERROR -> throw new IllegalArgumentException("Invalid outputData type for encoding!");
        };
    }

    private Data encodeArray() throws IOException {
        int arrayLength = encodeLength();
        if (arrayLength == -1) {
            return new ArrayData(arrayLength);
        }

        ArrayData arrayData = new ArrayData(arrayLength);

        for (int index = 0; index < arrayLength; index++) {
            Data encodedData = encodeData();
            arrayData.addData(encodedData);
        }

        return arrayData;
    }

    private Data encodeBulkString() throws IOException {
        int strLength = encodeLength();

        String data;
        if (strLength == -1) {
            data = null;   // If the length is -1, return the null string.
        } else if (strLength == 0) {
           data = "";   // If the length is 0, return the empty string.
        } else {

            // Read all string bytes from the buffer.
            byte[] strBytes = readNBytes(strLength);
            if (strBytes.length == 0) {
                throw new EOFException();
            }

            // Validate if we have read all bytes.
            if (strBytes.length != strLength) {
                throw new EOFException();
            }

            data = new String(strBytes, US_ASCII);
        }

        byte[] crlfBytes = readNBytes(2);
        validateCRLF(crlfBytes);

        return new BulkStringData(data);
    }

    private Data encodeSimpleString() throws IOException {
        StringBuilder sb = new StringBuilder();
        int currentByte = read();

        while (currentByte != CR) {
            sb.append((char) currentByte);
            currentByte = read();
        }

        byte[] crlfBytes = new byte[2];
        crlfBytes[0] = (byte) currentByte;
        crlfBytes[1] = (byte) read();

        validateCRLF(crlfBytes);

        return new SimpleStringData(sb.toString());
    }
}