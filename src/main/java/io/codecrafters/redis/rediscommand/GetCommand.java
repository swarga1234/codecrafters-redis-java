package io.codecrafters.redis.rediscommand;

import io.codecrafters.redis.protocol.RespValue;

import java.util.List;

public class GetCommand implements TypicalRedisCommand{
    @Override
    public int getMinArgs() {
        return 1;
    }

    @Override
    public RespValue execute(List<RespValue> args) {
        return null;
    }
}
