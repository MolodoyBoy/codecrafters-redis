package com.my.redis.context;

import static com.my.redis.context.ReplicationContext.ROLE.*;

public class ReplicationContext {

    private final int port;
    private final ROLE role;
    private final MasterConnection masterConnection;

    private int replicationOffset = -1;
    private String replicationId = "?";

    public ReplicationContext(int port, String masterURL) {
        this.port = port;

        if (masterURL != null) {
            this.role = SLAVE;
            this.masterConnection = new MasterConnection(masterURL);

        } else {
            this.role = MASTER;
            this.masterConnection = null;
        }
    }

    public int port() {
        return port;
    }

    public ROLE role() {
        return role;
    }

    public int getReplicationOffset() {
        return replicationOffset;
    }

    public String getReplicationId() {
        return replicationId;
    }

    public void setReplicationOffset(int replicationOffset) {
        this.replicationOffset = replicationOffset;
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