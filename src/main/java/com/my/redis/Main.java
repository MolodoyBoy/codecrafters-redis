package com.my.redis;

import com.my.redis.data_storage.key_space.KeySpaceStorage;
import com.my.redis.data_storage.list.ListDataStorage;
import com.my.redis.data_storage.map.MapDataStorage;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data_storage.replication.ReplicationAppendLog;
import com.my.redis.data_storage.stream.StreamDataStorage;
import com.my.redis.context.TransactionContext;
import com.my.redis.executor.base.RequestExecutor;
import com.my.redis.parser.ArgumentParser;
import com.my.redis.server.ReplicationSlaveClient;
import com.my.redis.system.ExpiredEntriesCleaner;
import com.my.redis.server.RedisServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.*;
import static java.util.concurrent.TimeUnit.*;

public class Main {

    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_WORKER_THREADS = 20;

    public static void main(String[] args) throws InterruptedException {
        ArgumentParser argumentParser = new ArgumentParser(args);
        int port = argumentParser.parsePortArg(DEFAULT_PORT);
        String masterURL = argumentParser.parseReplicaOfArg();

        KeySpaceStorage keySpaceStorage = new KeySpaceStorage();
        MapDataStorage dataStorage = new MapDataStorage(keySpaceStorage);
        ListDataStorage listDataStorage = new ListDataStorage(keySpaceStorage);
        StreamDataStorage streamDataStorage = new StreamDataStorage(keySpaceStorage);

        TransactionContext transactionContext = new TransactionContext();
        ReplicationContext replicationContext = new ReplicationContext(masterURL);

        RequestExecutor requestExecutor = new RequestExecutor(
            keySpaceStorage,
            dataStorage,
            listDataStorage,
            streamDataStorage,
            transactionContext,
            replicationContext
        );

        ReplicationSlaveClient replicationSlaveClient = new ReplicationSlaveClient(port, replicationContext);

        try (ExecutorService systemServerExecutor = newFixedThreadPool(2);
             ExecutorService redisServerExecutor = newFixedThreadPool(DEFAULT_WORKER_THREADS);
             ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor()) {

            ExpiredEntriesCleaner expiredEntriesCleaner = new ExpiredEntriesCleaner(100, dataStorage);
            RedisServer redisServer = new RedisServer(
                port,
                redisServerExecutor,
                requestExecutor,
                replicationContext,
                new ReplicationAppendLog()
            );

            scheduledExecutorService.scheduleWithFixedDelay(
                expiredEntriesCleaner,
                10,
                20,
                MINUTES
            );

            systemServerExecutor.submit(replicationSlaveClient);
            systemServerExecutor.submit(redisServer);

            Thread.currentThread().join();
        }
    }
}