package com.my.redis.executor.base;

import com.my.redis.Command;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.SimpleStringData;
import com.my.redis.data_storage.transaction.TransactionContext;
import com.my.redis.executor.args.CommandArgs;

public class TransactionalCommandExecutor implements CommandExecutor {

    private final CommandExecutor proxy;
    private final TransactionContext transactionContext;

    public TransactionalCommandExecutor(CommandExecutor proxy,
                                        TransactionContext transactionContext) {
        this.proxy = proxy;
        this.transactionContext = transactionContext;
    }


    @Override
    public Command supportedCommand() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        if (transactionContext.inTransaction()) {
            transactionContext.addCommandToQueue(
                () -> proxy.execute(commandArgs)
            );

            return new SimpleStringData("QUEUED").encode();
        }

        return proxy.execute(commandArgs);
    }
}