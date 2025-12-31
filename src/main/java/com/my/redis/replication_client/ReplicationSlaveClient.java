package com.my.redis.replication_client;

import com.my.redis.RedisResponse;
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

            try (BufferedOutputStream out = getBufferedWriter(socket);
                BufferedInputStream in = getBufferedInputStream(socket)) {
                RequestDataDecoder requestDataDecoder = new RequestDataDecoder(in);

                MasterSlaveHandshake handshake = new MasterSlaveHandshake(port, out, requestDataDecoder, replicationContext);
                handshake.performHandshake();

                System.out.println("Handshake with master completed.");

                RDBFileDecoder rdbFileDecoder = new RDBFileDecoder(in);
                byte[] masterRBDFileContent = rdbFileDecoder.encode();

                replicationContext.updateReplicationOffset(1);

                System.out.println("RDB file received.");

                replicationContext.silentDuringReplicationCommand(true);

                while (!executorService.isShutdown()) {
                    try {
                        Data data = requestDataDecoder.encode();
                        RedisResponse response = requestExecutor.execute(data);
                        replicationContext.updateReplicationOffset(data.encode().getBytes(StandardCharsets.US_ASCII).length);

                        if (!replicationContext.silentDuringReplicationCommand()) {
                            out.write(response.outputData().getBytes(StandardCharsets.US_ASCII));
                            out.flush();

                            // After processing each command, ensure silence during replication
                            replicationContext.silentDuringReplicationCommand(true);
                        }

                    } catch (EOFException e) {

                    } catch (Exception e) {
                        System.err.println("Exception while receiving replicated data: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("Exception while handling handshake: " + e.getMessage());
        }
    }

    private BufferedOutputStream getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedOutputStream(clientSocket.getOutputStream());
    }

    private BufferedInputStream getBufferedInputStream(Socket clientSocket) throws IOException {
        return new BufferedInputStream(clientSocket.getInputStream());
    }
}

