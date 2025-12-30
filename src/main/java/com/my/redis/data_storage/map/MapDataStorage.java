package com.my.redis.data_storage.map;

import com.my.redis.Utils;
import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.replication.ReplicationAppendLog;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static java.lang.System.*;
import static java.lang.Thread.*;

public class MapDataStorage {

    private static final Class<Data> CLASS = Data.class;

    private final Queue<KeyValuePair> queue;
    private final ReadWriteLock readWriteLock;
    private final KeySpaceStorage keySpaceStorage;
    private final ReplicationAppendLog replicationAppendLog;

    public MapDataStorage(KeySpaceStorage keySpaceStorage,
                          ReplicationAppendLog replicationAppendLog) {
        this.queue = new PriorityQueue<>();
        this.keySpaceStorage = keySpaceStorage;
        this.replicationAppendLog = replicationAppendLog;
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    /**
     *  Retrieves the value associated with the given key.
     *  If the value has expired, it is removed from the storage and null is returned.
     *  To perform better concurrency, the method was broken down into two parts:
     *  1. Read the value under a read lock.
     *  2. If expired, remove it under a write lock.
     */
    public String get(String key) {
        Supplier<Data> readTask = () -> keySpaceStorage.get(key, CLASS);

        Data valueData = executeWithLock(readWriteLock.readLock(), readTask);
        if (valueData == null) {
            return null;
        }

        long nowMillis = currentTimeMillis();
        Long expireAtMillis = valueData.expireAtMillis();
        if (expireAtMillis == null || nowMillis < expireAtMillis) {
            return valueData.value();
        }

        Supplier<Void> cleanupTask = () -> {
            keySpaceStorage.remove(key, valueData);
            return null;
        };

        executeWithLock(readWriteLock.writeLock(), cleanupTask);

        return null;
    }

    public int increment(String key, String query) {
        String value = get(key);

        readWriteLock.writeLock().lock();

        try {
            if (value == null) {
                int initialValue = 1;
                Data newData = new Data(Integer.toString(initialValue), null);
                keySpaceStorage.put(key, newData);
                return initialValue;
            }

            if (!Utils.isInteger(value)) {
                return -1;
            }

            Data data = keySpaceStorage.get(key, CLASS);
            int newValue = Utils.parseInt(value) + 1;

            Data newData = new Data(Integer.toString(newValue), data.expireAtMillis());
            keySpaceStorage.put(key, newData);

            return newValue;
        } finally {
            replicationAppendLog.add(query);
            readWriteLock.writeLock().unlock();
        }
    }

    public void put(String key, String value, Long expireAtMillis, String query) {
        try {
            Data valueData = new Data(value, expireAtMillis);
            if (expireAtMillis != null) {
                queue.add(new KeyValuePair(key, valueData));
            }

            keySpaceStorage.put(key, valueData);
        } finally {
            replicationAppendLog.add(query);
        }
    }

    /**
        * Removes up to maxToRemove expired entries from the storage.
        * @param maxToRemove limits the number of entries to remove in one call to avoid long blocking.
     */
    public void removeExpired(int maxToRemove) {
        Supplier<Void> task = () -> {
            long nowMillis = currentTimeMillis();

            int iterations = 0;
            KeyValuePair element = queue.peek();
            while (element != null && element.value().expireAtMillis() <= nowMillis && iterations < maxToRemove) {
                if (currentThread().isInterrupted()) {
                    break;
                }

                queue.poll();

                // Double check to avoid removing non-expired entries if they were updated
                keySpaceStorage.remove(element.key(), element.value());

                element = queue.peek();
                iterations++;
            }

            return null;
        };

        executeWithLock(readWriteLock.writeLock(), task);
    }

    private <T> T executeWithLock(Lock lock, Supplier<T> task) {
        lock.lock();

        try {
            return task.get();
        } finally {
            lock.unlock();
        }
    }

    private record KeyValuePair(String key, Data value) implements Comparable<KeyValuePair> {

        @Override
        public int compareTo(KeyValuePair o) {
            return this.value.expireAtMillis().compareTo(o.value.expireAtMillis());
        }
    }
}