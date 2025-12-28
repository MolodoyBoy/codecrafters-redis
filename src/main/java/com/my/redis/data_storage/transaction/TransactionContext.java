package com.my.redis.data_storage.transaction;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;

import static java.lang.ThreadLocal.*;

public class TransactionContext {

    private final ThreadLocal<DataHolder> threadLocal = withInitial(DataHolder::new);

    public void beginTransaction() {
        DataHolder dataHolder = threadLocal.get();
        dataHolder.setInTransaction(true);
    }

    public boolean inTransaction() {
        DataHolder dataHolder = threadLocal.get();
        if (dataHolder == null) {
            return false;
        }

        return dataHolder.inTransaction;
    }

    public void addCommandToQueue(Callable<String> runnable) {
        DataHolder dataHolder = threadLocal.get();
        if (dataHolder != null) {
            dataHolder.commandQueue.add(runnable);
        }
    }

    public Queue<Callable<String>> endTransaction() {
        DataHolder dataHolder = threadLocal.get();
        if (dataHolder != null) {
            Queue<Callable<String>> commandQueue = dataHolder.commandQueue;

            threadLocal.remove();
            return commandQueue;
        }

        throw new IllegalStateException("No transaction in progress!");
    }

    private static class DataHolder {

        private boolean inTransaction;
        private final Queue<Callable<String>> commandQueue;

        public DataHolder() {
            this.inTransaction = false;
            this.commandQueue = new LinkedList<>();
        }

        public void setInTransaction(boolean inTransaction) {
            this.inTransaction = inTransaction;
        }
    }
}