package com.my.redis.server;

import com.my.redis.RequestDataDecoder;
import com.my.redis.context.MasterAddress;
import com.my.redis.context.ReplicationContext;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class ReplicationSlaveClient implements Runnable {

    private final int port;
    private final ReplicationContext replicationContext;

    public ReplicationSlaveClient(int port, ReplicationContext replicationContext) {
        this.port = port;
        this.replicationContext = replicationContext;
    }

    @Override
    public void run() {
        if (replicationContext.role() == ReplicationContext.Role.MASTER) {
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

