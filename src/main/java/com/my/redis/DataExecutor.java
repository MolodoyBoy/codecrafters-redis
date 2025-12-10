package com.my.redis;

import com.my.redis.data.ArrayData;
import com.my.redis.data.Data;
import com.my.redis.data.DataType;
import com.my.redis.data.StringData;

import static com.my.redis.Command.*;
import static com.my.redis.Delimiter.*;
import static java.util.Arrays.*;

public class DataExecutor {

    public String execute(Data data) {
        DataType dataType = data.getType();

        return switch (dataType) {
            case NULL -> null;
            case ARRAY -> executeArray(data);
            case BULK_STRING, SIMPLE_STRING -> executeString(data);
        };
    }

    private String executeArray(Data data) {
        if (data instanceof ArrayData arrayData) {

            if (arrayData.isEmpty()) {
                return null;
            }

            Data[] arrayDataData = arrayData.getData();

            Data shouldBeCommand = arrayDataData[0];
            if (shouldBeCommand instanceof StringData stringData) {
                Command command = parseCommand(stringData.getValue());

                if (command == null) {
                    throw new IllegalArgumentException("Unknown command: " + stringData.getValue());
                }

                Data[] args;
                if (arrayDataData.length == 1) {
                    args = new Data[0];
                } else {
                    args = copyOfRange(arrayDataData, 1, arrayDataData.length);
                }

                return executeCommand(command, args);
            }
        }

        throw new IllegalArgumentException("Invalid data type for array execution!");
    }

    private String executeString(Data data) {
        if (data instanceof StringData stringData) {
            Command command = parseCommand(stringData.getValue());
            if (command == null) {
                throw new IllegalArgumentException("Unknown command: " + stringData.getValue());
            }

            return executeCommand(command, null);
        }

        throw new IllegalArgumentException("Invalid data type for string execution!");
    }

    private String executeCommand(Command command, Data[] args) {
        return switch (command) {
            case PING -> "+PONG" + CRLF;
            case ECHO -> {
                if (args == null || args.length != 1) {
                    throw new IllegalArgumentException("ECHO command requires one argument!");
                } else {
                    if (args[0] instanceof StringData stringData) {
                        yield stringData.decorate();
                    } else {
                        throw new IllegalArgumentException("ECHO argument must be a string!");
                    }
                }
            }
        };
    }
}