package com.my.redis.executor.transaction;

import com.my.redis.Command;
import com.my.redis.data.*;
import com.my.redis.data_storage.transaction.TransactionContext;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.util.Queue;
import java.util.concurrent.Callable;

import static com.my.redis.Command.*;

public class EXECCommandExecutor implements CommandExecutor {

    private final TransactionContext transactionContext;

    public EXECCommandExecutor(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    @Override
    public Command supportedCommand() {
        return EXEC;
    }

    @Override
    public boolean manageTransaction() {
        return true;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        boolean inTransaction = transactionContext.inTransaction();
        if (!inTransaction) {
            return new SimpleErrorData("ERR EXEC without MULTI").encode();
        }

        Queue<Callable<String>> commandQueue = transactionContext.endTransaction();

        ArrayData resultArray = new ArrayData(commandQueue.size());
        while (!commandQueue.isEmpty()) {

            try {
                Callable<String> command = commandQueue.poll();
                String result = command.call();

                resultArray.addData(new EncodedData(result));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        return resultArray.encode();
    }

    private record EncodedData(String data) implements Data {

        @Override
        public DataType getType() {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public String encode() {
            return data;
        }

        @Override
        public String getStringValue() {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
}
