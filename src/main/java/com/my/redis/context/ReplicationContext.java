package com.my.redis.context;

import static com.my.redis.context.ReplicationContext.ROLE.*;

public class ReplicationContext {

    private final ROLE role;
    private final String masterURL;

    public ReplicationContext(String masterURL) {
        if (masterURL != null) {
            this.role = SLAVE;
        } else {
            this.role = MASTER;
        }

        this.masterURL = masterURL;
    }

    public ROLE role() {
        return role;
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