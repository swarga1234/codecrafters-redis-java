package io.codecrafters.redis.rediscommand;

import io.codecrafters.redis.protocol.RespValue;

import java.util.List;

public interface TypicalRedisCommand {
    int getMinArgs();
    RespValue execute(List<RespValue> args);
}
