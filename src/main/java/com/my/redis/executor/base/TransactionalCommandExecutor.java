package com.my.redis.executor.base;

import com.my.redis.Command;
import com.my.redis.data.SimpleStringData;
import com.my.redis.context.TransactionContext;
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
        if (!proxy.needTransaction() || !transactionContext.inTransaction()) {
            return proxy.execute(commandArgs);
        }

        transactionContext.addCommandToQueue(() -> proxy.execute(commandArgs));

        return new SimpleStringData("QUEUED").encode();
    }
}