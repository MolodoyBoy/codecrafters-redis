package com.my.redis.executor.args;

import com.my.redis.Command;
import com.my.redis.data.Data;

public record CommandArgs(Command command, Data[] args, String inputData) {}