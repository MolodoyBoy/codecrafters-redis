package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.Data;
import com.my.redis.data.DataType;
import com.my.redis.data.StringData;
import com.my.redis.data_storage.ListDataStorage;
import com.my.redis.data_storage.MapDataStorage;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.*;
import static com.my.redis.Command.*;
import static java.util.function.Function.*;
import static java.util.stream.Collectors.*;

public class RequestExecutor {

    private final Map<Command, CommandExecutor> commandExecutors;

    public RequestExecutor(MapDataStorage mapDataStorage, ListDataStorage listDataStorage) {
        this.commandExecutors = Stream.of(
            new PingCommandExecutor(),
            new EchoCommandExecutor(),
            new SetCommandExecutor(mapDataStorage),
            new GetCommandExecutor(mapDataStorage),
            new LLENCommandExecutor(listDataStorage),
            new LPOPCommandExecutor(listDataStorage),
            new BLPOPCommandExecutor(listDataStorage),
            new RPUSHCommandExecutor(listDataStorage),
            new LPUSHCommandExecutor(listDataStorage),
            new LRANGECommandExecutor(listDataStorage),
            new TYPECommandExecutor(mapDataStorage, listDataStorage)
        ).collect(toMap(CommandExecutor::supportedCommand, identity()));
    }

    public String execute(Data data) {
        DataType dataType = data.getType();

        CommandArgs commandArgs = switch (dataType) {
            case NULL -> null;
            case ARRAY -> getArrayCommandArgs(data);
            case BULK_STRING, SIMPLE_STRING -> getStringCommandArgs(data);
            case INTEGER -> throw new IllegalArgumentException("Invalid command type: INTEGER!");
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