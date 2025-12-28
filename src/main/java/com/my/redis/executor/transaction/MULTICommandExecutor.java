package com.my.redis.executor.transaction;

import com.my.redis.Command;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data_storage.transaction.TransactionContext;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class MULTICommandExecutor implements CommandExecutor {

    private final TransactionContext transactionContext;

    public MULTICommandExecutor(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    @Override
    public Command supportedCommand() {
        return MULTI;
    }

    @Override
    public boolean manageTransaction() {
        return true;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        transactionContext.beginTransaction();
        return new SimpleStringData("OK").encode();
    }
}