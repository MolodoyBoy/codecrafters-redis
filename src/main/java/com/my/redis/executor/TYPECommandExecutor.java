package com.my.redis.executor;

import com.my.redis.Command;
import com.my.redis.data.Data;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data_storage.KeySpaceStorage;

import static com.my.redis.Command.TYPE;
import static com.my.redis.Utils.*;

public class TYPECommandExecutor implements CommandExecutor {

    private final KeySpaceStorage keySpaceStorage;

    public TYPECommandExecutor(KeySpaceStorage keySpaceStorage) {
        this.keySpaceStorage = keySpaceStorage;
    }

    @Override
    public Command supportedCommand() {
        return TYPE;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        if (args.length != 1) {
            throw new IllegalArgumentException("TYPE command requires exactly 1 argument!");
        }

        String key = parseString(args[0]);

        String type = keySpaceStorage.getType(key);
        return new SimpleStringData(type).encode();
    }
}
