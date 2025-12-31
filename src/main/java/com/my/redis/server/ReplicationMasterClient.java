package com.my.redis.server;

import com.my.redis.context.ReplicationContext;
import com.my.redis.data_storage.replication.ReplicationAppendLog;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutorService;

public class ReplicationMasterClient implements Runnable {

    private final Socket socket;
    private final ExecutorService executorService;
    private final ReplicationContext replicationContext;
    private final ReplicationAppendLog replicationAppendLog;

    public ReplicationMasterClient(Socket socket,
                                   ExecutorService executorService,
                                   ReplicationContext replicationContext,
                                   ReplicationAppendLog replicationAppendLog) {
        this.socket = socket;
        this.executorService = executorService;
        this.replicationContext = replicationContext;
        this.replicationAppendLog = replicationAppendLog;
    }

    @Override
    public void run() {
        if (replicationContext.role() == ReplicationContext.Role.SLAVE) {
            return;
        }

        try (BufferedOutputStream out = getBufferedWriter(socket)) {

            //shareRDBConfig(out);

            int currentOffset = replicationAppendLog.size();

            while (!executorService.isShutdown()) {

                try {
                    String result = replicationAppendLog.get(currentOffset++);

                    out.write(result.getBytes(StandardCharsets.US_ASCII));
                    out.flush();
                } catch (IOException e) {
                    System.err.println("IOException while sending replication data: " + e.getMessage());
                }
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("Exception in replication master client: " + e.getMessage());
        }
    }

    private void shareRDBConfig(BufferedOutputStream out) throws IOException {
        String rdbB64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] rdbBytes = Base64.getDecoder().decode(rdbB64);

        byte[] header = ("$" + rdbBytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII);

        byte[] payload = new byte[header.length + rdbBytes.length];
        System.arraycopy(header, 0, payload, 0, header.length);
        System.arraycopy(rdbBytes, 0, payload, header.length, rdbBytes.length);

        out.write(payload);
        out.flush();
    }

    private BufferedOutputStream getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedOutputStream(clientSocket.getOutputStream());
    }
}
