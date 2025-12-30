package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.BulkStringData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

public class INFOCommandExecutor implements CommandExecutor {

    private final ReplicationContext replicationContext;

    public INFOCommandExecutor(ReplicationContext replicationContext) {
        this.replicationContext = replicationContext;
    }

    @Override
    public Command supportedCommand() {
        return Command.INFO;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        ReplicationContext.ROLE role = replicationContext.role();
        String message = String.format("role:%s", role.getValue());

        return new BulkStringData(message).encode();
    }
}
