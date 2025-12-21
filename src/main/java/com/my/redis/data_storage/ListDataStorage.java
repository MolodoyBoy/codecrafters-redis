package com.my.redis.data_storage;

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

    private static final Class<ListStorageValue> CLASS = ListStorageValue.class;

    private final Condition condition;
    private final ReadWriteLock readWriteLock;
    private final KeySpaceStorage keySpaceStorage;

    public ListDataStorage(KeySpaceStorage keySpaceStorage) {
        this.keySpaceStorage = keySpaceStorage;
        this.readWriteLock = new ReentrantReadWriteLock(true);
        this.condition = readWriteLock.writeLock().newCondition();
    }

    public int length(String listKey) {
        readWriteLock.readLock().lock();

        try {
            ListStorageValue list = keySpaceStorage.get(listKey, CLASS);
            return list == null ? 0 : list.size();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public List<String> remove(String listKey, int count) {
        readWriteLock.writeLock().lock();

        try {
            ListStorageValue list = keySpaceStorage.get(listKey, CLASS);
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
            readWriteLock.writeLock().unlock();
        }
    }

    public int addToTail(String listKey, List<String> values) {
        readWriteLock.writeLock().lock();

        try {
            ListStorageValue list = keySpaceStorage.computeIfAbsent(listKey, new ListStorageValue(), CLASS);

            list.addAll(values);

            condition.signalAll();

            return list.size();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public int addToHead(String listKey, List<String> values) {
        readWriteLock.writeLock().lock();

        try {
            ListStorageValue list = keySpaceStorage.computeIfAbsent(listKey, new ListStorageValue(), CLASS);

            List<String> listValue = list.value;
            for (String value : values) {
                listValue.addFirst(value);
            }

            condition.signalAll();

            return list.size();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public Map.Entry<String, String> poll(List<String> listKeys, Duration duration) {
        long timeout = duration.toNanos();
        long nanosRemaining = duration.toNanos();

        readWriteLock.writeLock().lock();

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

            return entry(listKey, keySpaceStorage.get(listKey, CLASS).removeFirst());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private String findAnyNotEmptyList(List<String> listKeys) {
        for (String listKey : listKeys) {
            ListStorageValue list = keySpaceStorage.get(listKey, CLASS);
            if (list != null && !list.isEmpty()) {
                return listKey;
            }
        }

        return null;
    }

    public List<String> get(String listKey, int start, int stop) {
        readWriteLock.readLock().lock();

        try {
            ListStorageValue list = keySpaceStorage.get(listKey, CLASS);
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

    private record ListStorageValue(List<String> value) implements StorageValue {

        public ListStorageValue() {
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