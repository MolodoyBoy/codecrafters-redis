package com.my.redis.system;

import com.my.redis.DataDecoder;
import com.my.redis.data.Data;
import com.my.redis.executor.RequestExecutor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

public class RedisServer {

    private final int port;
    private final RequestExecutor requestExecutor;
    private final ExecutorService executorService;

    public RedisServer(int port, ExecutorService executorService, RequestExecutor requestExecutor) {
        this.port = port;
        this.requestExecutor = requestExecutor;
        this.executorService = executorService;
    }

    public void start() {
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

            while (!executorService.isShutdown()) {
                try {
                    DataDecoder dataDecoder = new DataDecoder(in);

                    Data data = dataDecoder.encode();
                    String resultMessage = requestExecutor.execute(data);

                    out.write(resultMessage);
                    out.flush();
                } catch (EOFException e) {

                } catch (IOException | IllegalArgumentException  e) {
                    System.err.println("Exception while handling client: " + e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private BufferedInputStream getBufferedInputStream(Socket clientSocket) throws IOException {
        return new BufferedInputStream(clientSocket.getInputStream());
    }

    private BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.US_ASCII));
    }
}