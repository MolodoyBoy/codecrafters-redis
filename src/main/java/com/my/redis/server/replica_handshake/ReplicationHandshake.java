package com.my.redis.server.replica_handshake;

import com.my.redis.context.ReplicationContext;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class ReplicationHandshake implements Runnable {

    private final ReplicationContext replicationContext;

    public ReplicationHandshake(ReplicationContext replicationContext) {
        this.replicationContext = replicationContext;
    }

    @Override
    public void run() {
        if (replicationContext.role() == ReplicationContext.ROLE.MASTER) {
            return;
        }

        String masterUrl = replicationContext.masterURL();
        if (masterUrl == null || masterUrl.isBlank()) {
            return;
        }

        InetSocketAddress socketAddress = parse(masterUrl);

        try (Socket socket = new Socket()) {
            socket.connect(socketAddress, 5000);

            try (BufferedWriter out = getBufferedWriter(socket)) {
                ArrayData arrayData = new ArrayData(1);
                arrayData.addData(new BulkStringData("PING"));

                out.write(arrayData.encode());
                out.flush();
            }

        } catch (IOException e) {
            System.err.println("Exception while handling handshake: " + e.getMessage());
        }
    }

    private BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.US_ASCII));
    }

    private InetSocketAddress parse(String masterUrl) {
        String[] parts = masterUrl.split(" ");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new InetSocketAddress(host, port);
    }
}

