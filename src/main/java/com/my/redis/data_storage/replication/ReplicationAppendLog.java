package com.my.redis.data_storage.replication;

import com.my.redis.context.ReplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicationAppendLog {

    private final List<String> log;
    private final ReentrantLock lock;
    private final Condition condition;
    private final ReplicationContext replicationContext;

    public ReplicationAppendLog(ReplicationContext replicationContext) {
        this.log = new ArrayList<>();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.replicationContext = replicationContext;
    }

    public int size() {
        lock.lock();
        try {
            return log.size();
        } finally {
            lock.unlock();
        }
    }

    public void add(String query) {
        if (!replicationContext.hasReplicas()) {
            return;
        }

        lock.lock();
        try {
            this.log.add(query);
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public String get(int currentOffset) throws InterruptedException {
        lock.lock();

        try {
            while (currentOffset >= log.size()) {
                condition.await();
            }

            return log.get(currentOffset);
        } finally {
            lock.unlock();
        }
    }
}