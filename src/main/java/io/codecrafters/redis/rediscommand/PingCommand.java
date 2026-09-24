package io.codecrafters.redis.rediscommand;

import io.codecrafters.redis.protocol.RespSimpleString;
import io.codecrafters.redis.protocol.RespValue;

import java.util.List;

public class PingCommand implements TypicalRedisCommand{
    @Override
    public RespValue execute(List<RespValue> args) {
        return new RespSimpleString("PONG");
    }
}
