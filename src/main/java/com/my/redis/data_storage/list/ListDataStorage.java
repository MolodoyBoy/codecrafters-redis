package com.my.redis.data_storage.list;

import com.my.redis.Command;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.key_space.Storage;
import com.my.redis.data_storage.replication.ReplicationAppendLog;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.Thread.*;
import static java.util.Map.*;

public class ListDataStorage {

    private static final Class<ListStorage> CLASS = ListStorage.class;

    private final Condition condition;
    private final ReadWriteLock readWriteLock;
    private final KeySpaceStorage keySpaceStorage;
    private final ReplicationAppendLog replicationAppendLog;

    public ListDataStorage(KeySpaceStorage keySpaceStorage,
                           ReplicationAppendLog replicationAppendLog) {
        this.keySpaceStorage = keySpaceStorage;
        this.replicationAppendLog = replicationAppendLog;
        this.readWriteLock = new ReentrantReadWriteLock(true);
        this.condition = readWriteLock.writeLock().newCondition();
    }

    public int length(String listKey) {
        readWriteLock.readLock().lock();

        try {
            ListStorage list = keySpaceStorage.get(listKey, CLASS);
            return list == null ? 0 : list.size();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public List<String> remove(String listKey, int count, String query) {
        readWriteLock.writeLock().lock();

        try {
            ListStorage list = keySpaceStorage.get(listKey, CLASS);
            if (list == null || list.isEmpty()) {
                return null;
            }

            if (count >= list.size()) {
                List<String> removed = new LinkedList<>(list.value());
                list.clear();

                return removed;
            }

            List<String> removed = new LinkedList<>();
            for (int i = 0; i < count; i++) {
                removed.add(list.removeFirst());
            }

            return removed;
        } finally {
            replicationAppendLog.add(query);
            readWriteLock.writeLock().unlock();
        }
    }

    public int addToTail(String listKey, List<String> values,  String query) {
        readWriteLock.writeLock().lock();

        try {
            ListStorage list = keySpaceStorage.computeIfAbsent(listKey, new ListStorage(), CLASS);

            list.addAll(values);

            condition.signalAll();

            return list.size();
        } finally {
            replicationAppendLog.add(query);
            readWriteLock.writeLock().unlock();
        }
    }

    public int addToHead(String listKey, List<String> values, String query) {
        readWriteLock.writeLock().lock();

        try {
            ListStorage list = keySpaceStorage.computeIfAbsent(listKey, new ListStorage(), CLASS);

            List<String> listValue = list.value;
            for (String value : values) {
                listValue.addFirst(value);
            }

            condition.signalAll();

            return list.size();
        } finally {
            replicationAppendLog.add(query);
            readWriteLock.writeLock().unlock();
        }
    }

    public Map.Entry<String, String> poll(List<String> listKeys, Duration duration) {
        long timeout = duration.toNanos();
        long nanosRemaining = duration.toNanos();

        readWriteLock.writeLock().lock();

        ArrayData arrayData = new ArrayData(2);
        arrayData.addData(new BulkStringData(Command.LPOP.command()));

        try {
            String listKey;

            while ((listKey = findAnyNotEmptyList(listKeys)) == null) {
                if (timeout == 0) {
                    condition.await();
                } else {
                    nanosRemaining = condition.awaitNanos(nanosRemaining);
                    if (nanosRemaining <= 0) {
                        return null;
                    }
                }
            }

            arrayData.addData(new BulkStringData(listKey));

            return entry(listKey, keySpaceStorage.get(listKey, CLASS).removeFirst());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        } finally {
            //Replace with LPOP for non blocking replication.
            replicationAppendLog.add(arrayData.encode());
            readWriteLock.writeLock().unlock();
        }
    }

    private String findAnyNotEmptyList(List<String> listKeys) {
        for (String listKey : listKeys) {
            ListStorage list = keySpaceStorage.get(listKey, CLASS);
            if (list != null && !list.isEmpty()) {
                return listKey;
            }
        }

        return null;
    }

    public List<String> get(String listKey, int start, int stop) {
        readWriteLock.readLock().lock();

        try {
            ListStorage list = keySpaceStorage.get(listKey, CLASS);
            if (list == null || list.isEmpty()) {
                return List.of();
            }

            int size = list.size();

            // Convert negative indices to positive
            // In Redis, -1 means the last element, -2 means second to last, etc.
            if (start < 0) {
                start = size + start;
            }

            if (stop < 0) {
                stop = size + stop;
            }

            if (start < 0) {
                start = 0;
            }

            if (start >= size) {
                return List.of();
            }

            if (stop >= size) {
                stop = size - 1;
            }

            if (stop < 0) {
                return List.of();
            }

            if (start > stop) {
                return List.of();
            }

            return list.value.subList(start, stop + 1);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private record ListStorage(List<String> value) implements Storage {

        public ListStorage() {
            this(new LinkedList<>());
        }

        public int size() {
            return value.size();
        }

        public void clear() {
            value.clear();
        }

        public void addAll(List<String> values) {
            value.addAll(values);
        }

        public String removeFirst() {
            return value.removeFirst();
        }

        public boolean isEmpty() {
            return value.isEmpty();
        }

        @Override
        public String type() {
            return "list";
        }
    }
}