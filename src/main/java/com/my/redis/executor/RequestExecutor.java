package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.Data;
import com.my.redis.data.DataType;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.MapDataStorage;

import java.util.Map;

import static java.util.Arrays.*;
import static com.my.redis.Command.*;

public class RequestExecutor {

    private final Map<Command, CommandExecutor> commandExecutors;

    public RequestExecutor() {
        MapDataStorage mapDataStorage = new MapDataStorage();

        this.commandExecutors = Map.of(
            PING, new PingCommandExecutor(),
            ECHO, new EchoCommandExecutor(),
            SET, new SetCommandExecutor(mapDataStorage),
            GET, new GetCommandExecutor(mapDataStorage)
        );
    }

    public String execute(Data data) {
        DataType dataType = data.getType();

        CommandArgs commandArgs = switch (dataType) {
            case NULL -> null;
            case ARRAY -> getArrayCommandArgs(data);
            case BULK_STRING, SIMPLE_STRING -> getStringCommandArgs(data);
        };

        if (commandArgs == null) {
            return null;
        }

        return commandExecutors.get(commandArgs.command()).execute(commandArgs);
    }

    private CommandArgs getArrayCommandArgs(Data data) {
        if (data instanceof ArrayData arrayData) {

            if (arrayData.isEmpty()) {
                return null;
            }

            Data[] arrayDataData = arrayData.getData();

            Data shouldBeCommand = arrayDataData[0];
            Command command = findCommand(shouldBeCommand);

            Data[] args;
            if (arrayDataData.length == 1) {
                args = new Data[0];
            } else {
                args = copyOfRange(arrayDataData, 1, arrayDataData.length);
            }

            return new CommandArgs(command, args);
        }

        throw new IllegalArgumentException("Invalid data type for array execution!");
    }

    private CommandArgs getStringCommandArgs(Data data) {
        Command command = findCommand(data);
        return new CommandArgs(command, null);
    }

    private Command findCommand(Data shouldBeCommand) {
        if (shouldBeCommand instanceof StringData stringData) {
            Command command = parseCommand(stringData.getValue());

            if (command != null) {
                return command;
            }
        }

        throw new IllegalArgumentException("Invalid command!");
    }
}