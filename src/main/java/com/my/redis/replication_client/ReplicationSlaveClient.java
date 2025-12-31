package com.my.redis.replication_client;

import com.my.redis.decoder.RDBFileDecoder;
import com.my.redis.decoder.RequestDataDecoder;
import com.my.redis.context.MasterAddress;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.Data;
import com.my.redis.executor.base.RequestExecutor;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

import static com.my.redis.context.ReplicationContext.Role.*;

public final class ReplicationSlaveClient implements Runnable {

    private final int port;
    private final RequestExecutor requestExecutor;
    private final ExecutorService executorService;
    private final ReplicationContext replicationContext;

    public ReplicationSlaveClient(int port,
                                  RequestExecutor requestExecutor,
                                  ExecutorService executorService,
                                  ReplicationContext replicationContext) {
        this.port = port;
        this.requestExecutor = requestExecutor;
        this.executorService = executorService;
        this.replicationContext = replicationContext;
    }

    @Override
    public void run() {
        if (replicationContext.role() != SLAVE) {
            return;
        }

        MasterAddress masterAddress = replicationContext.masterConnection();
        try (Socket socket = new Socket()) {
            socket.connect(masterAddress.getSocketAddress(), 5000);

            try (BufferedWriter out = getBufferedWriter(socket);
                BufferedInputStream in = getBufferedInputStream(socket)) {
                RequestDataDecoder requestDataDecoder = new RequestDataDecoder(in);

                MasterSlaveHandshake handshake = new MasterSlaveHandshake(port, out, requestDataDecoder, replicationContext);
                handshake.performHandshake();

                System.out.println("Handshake with master completed.");

                RDBFileDecoder rdbFileDecoder = new RDBFileDecoder(in);
                byte[] masterRBDFileContent = rdbFileDecoder.encode();

                System.out.println("RDB file received.");

                while (!executorService.isShutdown()) {
                    try {
                        Data data = requestDataDecoder.encode();
                        requestExecutor.execute(data);

                    } catch (EOFException e) {

                    } catch (IOException | IllegalArgumentException  e) {
                        System.err.println("Exception while handling client: " + e.getMessage());
                        e.printStackTrace();
                    } catch (Exception e) {
                        System.err.println("Unexpected exception: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("Exception while handling handshake: " + e.getMessage());
        }
    }

    private BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.US_ASCII));
    }

    private BufferedInputStream getBufferedInputStream(Socket clientSocket) throws IOException {
        return new BufferedInputStream(clientSocket.getInputStream());
    }
}

