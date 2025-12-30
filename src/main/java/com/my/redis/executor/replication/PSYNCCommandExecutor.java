package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.SimpleStringData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.my.redis.Command.*;

public class PSYNCCommandExecutor implements CommandExecutor {

    private final ReplicationContext replicationContext;

    public PSYNCCommandExecutor(ReplicationContext replicationContext) {
        this.replicationContext = replicationContext;
    }

    @Override
    public Command supportedCommand() {
        return PSYNC;
    }

    @Override
    public boolean needTransaction() {
        return false;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        String result = "FULLRESYNC "
            + replicationContext.getReplicationId()
            + " " + replicationContext.getReplicationOffset();

        return new SimpleStringData(result).encode();
    }

    @Override
    public byte[] executeAdditional(CommandArgs commandArgs) {
        String rdbB64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] rdbBytes = Base64.getDecoder().decode(rdbB64);

        byte[] header = ("$" + rdbBytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII);

        byte[] payload = new byte[header.length + rdbBytes.length];
        System.arraycopy(header, 0, payload, 0, header.length);
        System.arraycopy(rdbBytes, 0, payload, header.length, rdbBytes.length);

        return payload;
    }
}