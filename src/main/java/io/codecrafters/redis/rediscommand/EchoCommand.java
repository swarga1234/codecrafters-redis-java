package io.codecrafters.redis.rediscommand;

import io.codecrafters.redis.protocol.RespError;
import io.codecrafters.redis.protocol.RespValue;

import java.util.List;

public class EchoCommand implements TypicalRedisCommand{
    @Override
    public RespValue execute(List<RespValue> args) {
        if(args.isEmpty()){
            return new RespError("ERR wrong number of arguments for 'echo' command");
        }
        return args.getFirst();
    }
}
