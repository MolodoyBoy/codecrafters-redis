package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.IntegerData;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.*;

public class WAITCommandExecutor implements CommandExecutor {

    private final ReplicationContext replicationContext;

    public WAITCommandExecutor(ReplicationContext replicationContext) {
        this.replicationContext = replicationContext;
    }

    @Override
    public Command supportedCommand() {
        return WAIT;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        return new IntegerData(replicationContext.replicaNumber()).encode();
    }
}