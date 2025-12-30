package com.my.redis.context;

import java.util.concurrent.atomic.AtomicInteger;

import static com.my.redis.context.ReplicationContext.ROLE.*;

public class ReplicationContext {

    private final int port;
    private final ROLE role;
    private final MasterConnection masterConnection;

    private String replicationId;
    private final AtomicInteger replicationOffset;

    public ReplicationContext(int port, String masterURL) {
        this.port = port;

        if (masterURL != null) {
            this.role = SLAVE;
            this.replicationId = "?";
            this.replicationOffset = new AtomicInteger(-1);
            this.masterConnection = new MasterConnection(masterURL);

        } else {
            this.role = MASTER;
            this.masterConnection = null;
            this.replicationOffset = new AtomicInteger(0);
            this.replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        }
    }

    public int port() {
        return port;
    }

    public ROLE role() {
        return role;
    }

    public int getReplicationOffset() {
        return replicationOffset.get();
    }

    public String getReplicationId() {
        return replicationId;
    }

    public void setReplicationOffset(int replicationOffset) {
        this.replicationOffset.set(replicationOffset);
    }

    public void setReplicationId(String replicationId) {
        this.replicationId = replicationId;
    }

    public MasterConnection masterConnection() {
        return masterConnection;
    }

    public enum ROLE {
        MASTER("master"),
        SLAVE("slave");

        private final String value;

        ROLE(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}