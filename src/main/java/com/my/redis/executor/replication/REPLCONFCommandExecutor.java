package com.my.redis.executor.replication;

import com.my.redis.Command;
import com.my.redis.context.ReplicationContext;
import com.my.redis.data.*;
import com.my.redis.executor.args.CommandArgs;
import com.my.redis.executor.base.CommandExecutor;

import static com.my.redis.Command.REPLCONF;

public class REPLCONFCommandExecutor implements CommandExecutor {

    private final ReplicationContext replicationContext;

    public REPLCONFCommandExecutor(ReplicationContext replicationContext) {
        this.replicationContext = replicationContext;
    }

    @Override
    public Command supportedCommand() {
        return REPLCONF;
    }

    @Override
    public boolean needTransaction() {
        return false;
    }

    @Override
    public String execute(CommandArgs commandArgs) {
        Data[] args = commandArgs.args();
        Command command = commandArgs.command();

        Data option = args[0];
        if (option.getStringValue().equals("GETACK")) {
            replicationContext.silentDuringReplicationCommand(false);

            ArrayData arrayData = new ArrayData(3);
            arrayData.addData(new BulkStringData(command.command()));
            arrayData.addData(new BulkStringData("ACK"));
            arrayData.addData(new BulkStringData(Integer.toString(replicationContext.getReplicationOffset())));

            return arrayData.encode();
        }

        return new SimpleStringData("OK").encode();
    }
}