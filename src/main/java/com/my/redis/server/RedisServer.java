package com.my.redis.server;

import com.my.redis.RedisResponse;
import com.my.redis.RequestDataDecoder;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.Data;
import com.my.redis.data_storage.replication.ReplicationAppendLog;
import com.my.redis.executor.base.RequestExecutor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

public class RedisServer implements Runnable {

    private final int port;
    private final RequestExecutor requestExecutor;
    private final ExecutorService executorService;
    private final ReplicationContext replicationContext;
    private final ReplicationAppendLog replicationAppendLog;

    public RedisServer(int port,
                       ExecutorService executorService,
                       RequestExecutor requestExecutor,
                       ReplicationContext replicationContext,
                       ReplicationAppendLog replicationAppendLog) {
        this.port = port;
        this.requestExecutor = requestExecutor;
        this.executorService = executorService;
        this.replicationContext = replicationContext;
        this.replicationAppendLog = replicationAppendLog;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket()) {

            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));

            while (!executorService.isShutdown()) {
                Socket clientSocket = serverSocket.accept();

                executorService.submit(() -> {
                    try {
                        handleClient(clientSocket);
                    } catch (Exception e) {
                        System.out.println("IOException in client handler: " + e.getMessage());
                    } finally {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            System.out.println("IOException while closing client socket: " + e.getMessage());
                        }
                    }
                });
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private void handleClient(Socket clientSocket) throws IOException {
        System.out.println("Client handled: " + clientSocket.getRemoteSocketAddress());

        try (BufferedInputStream in = getBufferedInputStream(clientSocket);
             BufferedOutputStream out = getBufferedWriter(clientSocket)) {

            while (!executorService.isShutdown()) {
                try {
                    RequestDataDecoder requestDataDecoder = new RequestDataDecoder(in);

                    Data data = requestDataDecoder.encode();
                    RedisResponse result = requestExecutor.execute(data);

                    out.write(result.outputData().getBytes(StandardCharsets.US_ASCII));
                    out.flush();

                    if (replicationContext.isPropagated()) {
                        break;
                    }

                } catch (EOFException e) {

                } catch (IOException | IllegalArgumentException  e) {
                    System.err.println("Exception while handling client: " + e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }

            if (replicationContext.isPropagated()) {
                new ReplicationMasterClient(clientSocket, executorService, replicationContext, replicationAppendLog).run();
            }
        }
    }

    private BufferedInputStream getBufferedInputStream(Socket clientSocket) throws IOException {
        return new BufferedInputStream(clientSocket.getInputStream());
    }

    private BufferedOutputStream getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedOutputStream(clientSocket.getOutputStream());
    }
}