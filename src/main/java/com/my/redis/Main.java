package com.my.redis;

public class Main {

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;
        int workerThreads = 4;

        RedisServer redisServer = new RedisServer(port, workerThreads);
        redisServer.start();
    }
}
