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
import com.my.redis.replication_client.ReplicationSlaveClient;
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

        ReplicationContext replicationContext = new ReplicationContext(masterURL);
        ReplicationAppendLog replicationAppendLog = new ReplicationAppendLog(replicationContext);

        KeySpaceStorage keySpaceStorage = new KeySpaceStorage();
        MapDataStorage dataStorage = new MapDataStorage(keySpaceStorage, replicationAppendLog);
        ListDataStorage listDataStorage = new ListDataStorage(keySpaceStorage, replicationAppendLog);
        StreamDataStorage streamDataStorage = new StreamDataStorage(keySpaceStorage, replicationAppendLog);

        TransactionContext transactionContext = new TransactionContext();

        RequestExecutor requestExecutor = new RequestExecutor(
            keySpaceStorage,
            dataStorage,
            listDataStorage,
            streamDataStorage,
            transactionContext,
            replicationContext
        );

        try (ExecutorService systemServerExecutor = newFixedThreadPool(2);
             ExecutorService redisServerExecutor = newFixedThreadPool(DEFAULT_WORKER_THREADS);
             ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor()) {

            ExpiredEntriesCleaner expiredEntriesCleaner = new ExpiredEntriesCleaner(100, dataStorage);
            RedisServer redisServer = new RedisServer(
                port,
                redisServerExecutor,
                requestExecutor,
                replicationContext,
                replicationAppendLog
            );

            ReplicationSlaveClient replicationSlaveClient = new ReplicationSlaveClient(
                port,
                requestExecutor,
                systemServerExecutor,
                replicationContext
            );

            systemServerExecutor.submit(redisServer);
            systemServerExecutor.submit(replicationSlaveClient);
            scheduledExecutorService.scheduleWithFixedDelay(expiredEntriesCleaner, 10, 20, MINUTES);

            Thread.currentThread().join();
        }
    }
}