package com.my.redis.system;

import com.my.redis.data_storage.map.MapDataStorage;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.*;

public class ExpiredEntriesCleaner implements Runnable {

    private final int maxToRemove;
    private final MapDataStorage mapDataStorage;

    public ExpiredEntriesCleaner(int maxToRemove, MapDataStorage mapDataStorage) {
        this.maxToRemove = maxToRemove;
        this.mapDataStorage = mapDataStorage;
    }

    @Override
    public void run() {
        mapDataStorage.removeExpired(maxToRemove);
    }
}