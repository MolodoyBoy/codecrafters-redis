package com.my.redis;

import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.list.ListDataStorage;
import com.my.redis.data_storage.map.MapDataStorage;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.data_storage.transaction.TransactionContext;
import com.my.redis.executor.base.RequestExecutor;
import com.my.redis.parser.ArgumentParser;
import com.my.redis.system.ExpiredEntriesCleaner;
import com.my.redis.system.RedisServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.*;

public class Main {

    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_WORKER_THREADS = 10;

    public static void main(String[] args) {
        ArgumentParser argumentParser = new ArgumentParser();
        int port = argumentParser.parsePortArg(args, DEFAULT_PORT);

        KeySpaceStorage keySpaceStorage = new KeySpaceStorage();
        MapDataStorage dataStorage = new MapDataStorage(keySpaceStorage);
        ListDataStorage listDataStorage = new ListDataStorage(keySpaceStorage);
        StreamDataStorage streamDataStorage = new StreamDataStorage(keySpaceStorage);
        TransactionContext transactionContext = new TransactionContext();

        RequestExecutor requestExecutor = new RequestExecutor(
            keySpaceStorage,
            dataStorage,
            listDataStorage,
            streamDataStorage,
            transactionContext
        );

        try (ExecutorService executorService = newFixedThreadPool(DEFAULT_WORKER_THREADS);
             ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor()) {

            ExpiredEntriesCleaner expiredEntriesCleaner = new ExpiredEntriesCleaner(100, dataStorage, scheduledExecutorService);
            RedisServer redisServer = new RedisServer(port, executorService, requestExecutor);

            expiredEntriesCleaner.start();
            redisServer.start();
        }
    }
}