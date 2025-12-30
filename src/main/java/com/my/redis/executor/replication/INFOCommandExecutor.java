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
        String message = String.format(
            "role:%s," +
            "master_replid:%s," +
            "master_repl_offset:%s",
            role.getValue(),
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            0
        );

        return new BulkStringData(message).encode();
    }
}
