package com.my.redis.decoder;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;

import static com.my.redis.Delimiter.*;

public abstract class BaseDecoder {

    private final BufferedInputStream in;

    protected BaseDecoder(BufferedInputStream in) {
        this.in = in;
    }

    protected int encodeLength() throws IOException {
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

    protected int toDigit(int value) {
        int digit = value - '0';
        if (!isDigit(digit)) {
            throw new IllegalArgumentException("Invalid digit format!");
        }

        return digit;
    }

    protected boolean isDigit(int value) {
        return value >= 0 && value <= 9;
    }

    protected void validateCRLF(byte[] crlfBytes) throws IOException {
        if (crlfBytes.length != 2) {
            throw new EOFException("Unexpected end of stream!");
        }

        if (crlfBytes[0] != CR ||crlfBytes[1] != LF) {  // Skip '\r' and '\n'
            throw new IllegalArgumentException("Invalid array format!");
        }
    }

    protected int read() throws IOException {
        int read = in.read();
        if (read == -1) {
            throw new EOFException("Unexpected end of stream!");
        }

        return read;
    }

    protected byte[] readNBytes(int n) throws IOException {
        byte[] bytes = in.readNBytes(n);
        if (bytes.length != n) {
            throw new EOFException("Unexpected end of stream!");
        }

        return bytes;
    }
}