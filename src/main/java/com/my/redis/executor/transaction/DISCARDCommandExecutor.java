package com.my.redis.executor.transaction;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.SimpleErrorData;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data_storage.transaction.TransactionContext;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class DISCARDCommandExecutor implements CommandExecutor {

    private final TransactionContext transactionContext;

    public DISCARDCommandExecutor(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    @Override
    public Command supportedCommand() {
        return DISCARD;
    }

    @Override
    public boolean manageTransaction() {
        return true;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        boolean transactionDiscarded = transactionContext.discardTransaction();
        if (!transactionDiscarded) {
            return new SimpleErrorData("ERR DISCARD without MULTI").encode();
        }

        return new SimpleStringData("OK").encode();
    }
}