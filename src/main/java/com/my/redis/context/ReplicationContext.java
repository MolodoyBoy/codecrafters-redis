package com.my.redis.context;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.my.redis.context.ReplicationContext.Role.*;

public class ReplicationContext {

    private final Role role;
    private final AtomicBoolean hasReplicas;
    private final MasterAddress masterAddress;
    private final ThreadLocal<Boolean> propagated;
    private final AtomicInteger replicationOffset;
    private final AtomicReference<String> replicationId;
    private final ThreadLocal<Boolean> silentDuringReplicationCommand;

    public ReplicationContext(String masterURL) {
        this.hasReplicas = new AtomicBoolean(false);
        this.propagated = ThreadLocal.withInitial(() -> false);
        this.silentDuringReplicationCommand = ThreadLocal.withInitial(() -> null);

        if (masterURL != null) {
            this.role = SLAVE;
            this.replicationId = new AtomicReference<>("?");
            this.replicationOffset = new AtomicInteger(-1);
            this.masterAddress = new MasterAddress(masterURL);
        } else {
            this.role = MASTER;
            this.masterAddress = null;
            this.replicationOffset = new AtomicInteger(0);
            this.replicationId = new AtomicReference<>("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        }
    }

    public Role role() {
        return role;
    }

    public int getReplicationOffset() {
        return replicationOffset.get();
    }

    public String getReplicationId() {
        return replicationId.get();
    }

    public void updateReplicationOffset(int delta) {
        this.replicationOffset.addAndGet(delta);
    }

    public void setReplicationId(String replicationId) {
        this.replicationId.set(replicationId);
    }

    public void propagate() {
        if (role == SLAVE) {
            throw new UnsupportedOperationException("Slave cannot propagate as master.");
        }

        propagated.set(true);
        hasReplicas.set(true);
    }

    public boolean isPropagated() {
        return propagated.get();
    }

    public boolean hasReplicas() {
        return hasReplicas.get();
    }

    public void silentDuringReplicationCommand(boolean isReplication) {
        silentDuringReplicationCommand.set(isReplication);
    }

    public boolean silentDuringReplicationCommand() {
        Boolean result = silentDuringReplicationCommand.get();
        return result != null && result;
    }

    public MasterAddress masterConnection() {
        return masterAddress;
    }

    public enum Role {
        MASTER("master"),
        SLAVE("slave");

        private final String value;

        Role(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}