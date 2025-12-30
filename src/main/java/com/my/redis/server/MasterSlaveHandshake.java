package com.my.redis.server;

import com.my.redis.RequestDataDecoder;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.ArrayData;
import com.my.redis.data.BulkStringData;
import com.my.redis.data.Data;

import java.io.BufferedWriter;
import java.io.IOException;

import static com.my.redis.Command.*;

public class MasterSlaveHandshake {

    private final int port;
    private final BufferedWriter out;
    private final RequestDataDecoder requestDataDecoder;
    private final ReplicationContext replicationContext;

    public MasterSlaveHandshake(int port,
                                BufferedWriter out,
                                RequestDataDecoder requestDataDecoder,
                                ReplicationContext replicationContext) {
        this.out = out;
        this.port = port;
        this.requestDataDecoder = requestDataDecoder;
        this.replicationContext = replicationContext;
    }

    public void performHandshake() throws IOException {
        firstStep();
        secondStep();
        thirdStep();
    }

    private void firstStep() throws IOException {
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

    private void secondStep() throws IOException {
        ArrayData arrayData1 = new ArrayData(3);
        arrayData1.addData(new BulkStringData(REPLCONF.command()));
        arrayData1.addData(new BulkStringData("listening-port"));
        arrayData1.addData(new BulkStringData(Integer.toString(port)));

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

    private void thirdStep() throws IOException {
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
}
