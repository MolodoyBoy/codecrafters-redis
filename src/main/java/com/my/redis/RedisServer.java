package com.my.redis;

import com.my.redis.data.Data;
import com.my.redis.executor.RequestExecutor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {

    private final int port;
    private final int workerThreads;
    private final RequestExecutor requestExecutor;

    public RedisServer(int port, int workerThreads) {
        this.port = port;
        this.workerThreads = workerThreads;
        this.requestExecutor = new RequestExecutor();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket();
             ExecutorService executorService = Executors.newFixedThreadPool(workerThreads)) {

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
                            if (!clientSocket.isClosed()) {
                                clientSocket.close();
                            }
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
             BufferedWriter out = getBufferedWriter(clientSocket)) {

            while (true) {
                try {
                    DataEncoder dataEncoder = new DataEncoder(in);

                    Data data = dataEncoder.encode();
                    String resultMessage = requestExecutor.execute(data);

                    out.write(resultMessage);
                    out.flush();
                } catch (IOException | IllegalArgumentException  e) {
                    System.err.println("Exception while handling client: " + e.getMessage());
                    e.printStackTrace();
                    break;
                } catch (EndOfStreamException e) {
                   // Client ended the stream.
                }
            }
        }
    }

    private BufferedInputStream getBufferedInputStream(Socket clientSocket) throws IOException {
        return new BufferedInputStream(clientSocket.getInputStream());
    }

    private BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
    }
}