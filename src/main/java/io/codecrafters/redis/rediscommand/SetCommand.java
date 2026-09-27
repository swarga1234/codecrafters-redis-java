package io.codecrafters.redis.rediscommand;

import io.codecrafters.redis.protocol.RespError;
import io.codecrafters.redis.protocol.RespSimpleString;
import io.codecrafters.redis.protocol.RespValue;

import java.util.List;

public class SetCommand implements TypicalRedisCommand{
    @Override
    public int getMinArgs() {
        return 1;
    }

    @Override
    public RespValue execute(List<RespValue> args) {
        if(args.isEmpty()){
            return new RespError("ERR wrong number of arguments for 'SET' command");
        }

        return new RespSimpleString("OK");
    }
}
