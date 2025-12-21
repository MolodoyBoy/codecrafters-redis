package com.my.redis;

import com.my.redis.data.Data;
import com.my.redis.data.StringData;

public class Utils {

    public static int parseInt(StringData stringData) {
        try {
            return Integer.parseInt(stringData.getValue());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected integer value but got: " + stringData.getValue());
        }
    }

    public static Double parseDouble(StringData stringData) {
        try {
            return Double.parseDouble(stringData.getValue());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected integer value but got: " + stringData.getValue());
        }
    }

    public static String parseString(Data data) {
        if (data instanceof StringData stringData) {
            return stringData.getValue();
        }

        throw new IllegalArgumentException("Expected StringData type");
    }

    public static StringData toStringData(Data data) {
        if (data instanceof StringData stringData) {
            return stringData;
        }

        throw new IllegalArgumentException("Expected StringData type");
    }
}
