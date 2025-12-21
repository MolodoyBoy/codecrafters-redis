package com.my.redis;

import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.list.ListDataStorage;
import com.my.redis.data_storage.map.MapDataStorage;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.executor.base.RequestExecutor;
import com.my.redis.system.ExpiredEntriesCleaner;
import com.my.redis.system.RedisServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.*;

public class Main {

    public static void main(String[] args) {
        int port = 6379;
        int workerThreads = 10;

        KeySpaceStorage keySpaceStorage = new KeySpaceStorage();
        MapDataStorage dataStorage = new MapDataStorage(keySpaceStorage);
        ListDataStorage listDataStorage = new ListDataStorage(keySpaceStorage);
        StreamDataStorage streamDataStorage = new StreamDataStorage(keySpaceStorage);

        RequestExecutor requestExecutor = new RequestExecutor(keySpaceStorage, dataStorage, listDataStorage, streamDataStorage);

        try (ExecutorService executorService = newFixedThreadPool(workerThreads);
             ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor()) {

            ExpiredEntriesCleaner expiredEntriesCleaner = new ExpiredEntriesCleaner(100, dataStorage, scheduledExecutorService);
            RedisServer redisServer = new RedisServer(port, executorService, requestExecutor);

            expiredEntriesCleaner.start();
            redisServer.start();
        }
    }
}