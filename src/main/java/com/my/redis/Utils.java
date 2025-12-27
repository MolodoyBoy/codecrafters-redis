package com.my.redis;

import com.my.redis.data.Data;
import com.my.redis.data.StringData;

public class Utils {

    public static boolean isStreamId(String data) {
        try {

            String[] split = data.split("-");
            if (split.length == 1) {
                Integer.parseInt(split[0]);
            }

            if (split.length == 2) {
                Integer.parseInt(split[0]);
                Integer.parseInt(split[1]);
            }

            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

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

    public static Long parseLong(String data) {
        try {
            return Long.parseLong(data);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected integer value but got: " + data);
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
