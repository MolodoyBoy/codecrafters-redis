package com.my.redis;

import com.my.redis.data.*;

import java.io.IOException;
import java.io.EOFException;
import java.io.BufferedInputStream;

import static com.my.redis.Delimiter.*;
import static com.my.redis.data.DataType.*;
import static java.nio.charset.StandardCharsets.*;

public class DataEncoder {

    private final BufferedInputStream in;

    public DataEncoder(BufferedInputStream in) {
        this.in = in;
    }

    public Data encode() {
        try {
            int firstRead = read();

            return encodeData(firstRead);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid data format!", e);
        }
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
            throw new IllegalArgumentException("Invalid data type!");
        }

        return switch (dataType) {
            case NULL -> new NullData();
            case ARRAY -> encodeArray();
            case INTEGER -> throw new IllegalArgumentException("Invalid data type for integer encoding!");
            case BULK_STRING -> encodeBulkString();
            case SIMPLE_STRING -> encodeSimpleString();
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
            arrayData.addData(index, encodedData);
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

    private int encodeLength() throws IOException {
        int firstArrayLengthDigit = read();

        int arrayLength;
        int currentByte;
        if (firstArrayLengthDigit == MINUS) {

            // check if the next byte is '1' for -1 value.
            if (toDigit(read()) != 1) {
                throw new IllegalArgumentException("Invalid array format!");
            }

            arrayLength = -1;
            currentByte = read();

        } else {
            arrayLength = toDigit(firstArrayLengthDigit);
            currentByte = read();

            if (arrayLength != 0) {
                while (currentByte != CR) {
                    arrayLength = arrayLength * 10 + toDigit(currentByte);
                    currentByte = read();
                }
            }
        }

        byte[] crlfBytes = new byte[2];
        crlfBytes[0] = (byte) currentByte;
        crlfBytes[1] = (byte) read();

        validateCRLF(crlfBytes);

        return arrayLength;
    }

    private int toDigit(int value) {
        int digit = value - '0';
        if (!isDigit(digit)) {
            throw new IllegalArgumentException("Invalid digit format!");
        }

        return digit;
    }

    private boolean isDigit(int value) {
        return value >= 0 && value <= 9;
    }

    private void validateCRLF(byte[] crlfBytes) throws IOException {
        if (crlfBytes.length != 2) {
            throw new EOFException("Unexpected end of stream!");
        }

        if (crlfBytes[0] != CR ||crlfBytes[1] != LF) {  // Skip '\r' and '\n'
            throw new IllegalArgumentException("Invalid array format!");
        }
    }

    private int read() throws IOException {
        int read = in.read();
        if (read == -1) {
            throw new EOFException("Unexpected end of stream!");
        }

        return read;
    }

    private byte[] readNBytes(int n) throws IOException {
        byte[] bytes = in.readNBytes(n);
        if (bytes.length != n) {
            throw new EOFException("Unexpected end of stream!");
        }

        return bytes;
    }
}