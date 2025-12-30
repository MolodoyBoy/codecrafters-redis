package com.my.redis.server;

import com.my.redis.RequestDataDecoder;
import com.my.redis.context.MasterConnection;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static com.my.redis.Command.*;

public final class ReplicationHandshakeClient implements Runnable {

    private final ReplicationContext replicationContext;

    public ReplicationHandshakeClient(ReplicationContext replicationContext) {
        this.replicationContext = replicationContext;
    }

    @Override
    public void run() {
        if (replicationContext.role() == ReplicationContext.ROLE.MASTER) {
            return;
        }

        MasterConnection masterConnection = replicationContext.masterConnection();
        try (Socket socket = new Socket()) {
            socket.connect(masterConnection.getSocketAddress(), 5000);

            try (BufferedWriter out = getBufferedWriter(socket);
                BufferedInputStream in = getBufferedInputStream(socket)) {
                RequestDataDecoder requestDataDecoder = new RequestDataDecoder(in);

                firstStep(out, requestDataDecoder);
                secondStep(out, requestDataDecoder);
                thirdStep(out, requestDataDecoder);
            }

        } catch (IOException e) {
            System.err.println("Exception while handling handshake: " + e.getMessage());
        }
    }

    private void firstStep(BufferedWriter out, RequestDataDecoder requestDataDecoder) throws IOException {
        ArrayData arrayData = new ArrayData(1);
        arrayData.addData(new BulkStringData(PING.command()));

        out.write(arrayData.encode());
        out.flush();

        Data encode = requestDataDecoder.encode();

        String response = encode.getStringValue();
        if (!response.equals("PONG")) {
            throw new IOException("Invalid PING response from master: " + response);
        }
    }

    private void secondStep(BufferedWriter out, RequestDataDecoder requestDataDecoder) throws IOException {
        ArrayData arrayData1 = new ArrayData(3);
        arrayData1.addData(new BulkStringData(REPLCONF.command()));
        arrayData1.addData(new BulkStringData("listening-port"));
        arrayData1.addData(new BulkStringData(Integer.toString(replicationContext.port())));

        out.write(arrayData1.encode());
        out.flush();

        Data encode1 = requestDataDecoder.encode();
        String response1 = encode1.getStringValue();
        if (!response1.equals("OK")) {
            throw new IOException("Invalid REPLCONF response from master: " + response1);
        }

        ArrayData arrayData2 = new ArrayData(3);
        arrayData2.addData(new BulkStringData(REPLCONF.command()));
        arrayData2.addData(new BulkStringData("capa"));
        arrayData2.addData(new BulkStringData("psync2"));

        out.write(arrayData2.encode());
        out.flush();

        Data encode2 = requestDataDecoder.encode();
        String response2 = encode2.getStringValue();
        if (!response2.equals("OK")) {
            throw new IOException("Invalid REPLCONF response from master: " + response2);
        }
    }

    private void thirdStep(BufferedWriter out, RequestDataDecoder requestDataDecoder) throws IOException {
        ArrayData arrayData3 = new ArrayData(3);
        arrayData3.addData(new BulkStringData(PSYNC.command()));
        arrayData3.addData(new BulkStringData(replicationContext.getReplicationId()));
        arrayData3.addData(new BulkStringData(Integer.toString(replicationContext.getReplicationOffset())));

        out.write(arrayData3.encode());
        out.flush();

        Data encode3 = requestDataDecoder.encode();
        String response3 = encode3.getStringValue();

        if (!response3.startsWith("FULLRESYNC")) {
            throw new IOException("Invalid PSYNC response from master: " + response3);
        }
    }

    private BufferedWriter getBufferedWriter(Socket clientSocket) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.US_ASCII));
    }

    private BufferedInputStream getBufferedInputStream(Socket clientSocket) throws IOException {
        return new BufferedInputStream(clientSocket.getInputStream());
    }
}

