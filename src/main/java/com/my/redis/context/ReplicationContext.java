package com.my.redis.context;

import static com.my.redis.context.ReplicationContext.ROLE.*;

public class ReplicationContext {

    private final int port;
    private final ROLE role;
    private final String masterHost;
    private final Integer masterPort;

    public ReplicationContext(int port, String masterURL) {
        this.port = port;

        if (masterURL != null) {
            this.role = SLAVE;

            String[] split = masterURL.split(" ");

            this.masterHost = split[0];
            this.masterPort = Integer.parseInt(split[1]);

        } else {
            this.role = MASTER;

            this.masterHost = null;
            this.masterPort = null;
        }
    }

    public int port() {
        return port;
    }

    public ROLE role() {
        return role;
    }

    public String masterHost() {
        return masterHost;
    }

    public Integer masterPort() {
        return masterPort;
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