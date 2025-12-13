package com.my.redis.system;

import com.my.redis.data_storage.MapDataStorage;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.*;

public class ExpiredEntriesCleaner {

    private final int maxToRemove;
    private final MapDataStorage mapDataStorage;
    private final ScheduledExecutorService scheduledExecutorService;

    public ExpiredEntriesCleaner(int maxToRemove, MapDataStorage mapDataStorage, ScheduledExecutorService scheduledExecutorService) {
        this.maxToRemove = maxToRemove;
        this.mapDataStorage = mapDataStorage;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void start() {
        scheduledExecutorService.scheduleWithFixedDelay(
                () -> mapDataStorage.removeExpired(maxToRemove),
                10,
                20,
                MINUTES
        );
    }
}