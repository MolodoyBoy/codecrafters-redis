package com.my.redis.data_storage.transaction;

import java.util.LinkedList;
import java.util.Queue;

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

    public void addCommandToQueue(Runnable runnable) {
        DataHolder dataHolder = threadLocal.get();
        if (dataHolder != null) {
            dataHolder.commandQueue.add(runnable);
        }
    }

    private class DataHolder {

        private boolean inTransaction;
        private final Queue<Runnable> commandQueue;

        public DataHolder() {
            this.inTransaction = false;
            this.commandQueue = new LinkedList<>();
        }

        public void setInTransaction(boolean inTransaction) {
            this.inTransaction = inTransaction;
        }
    }
}