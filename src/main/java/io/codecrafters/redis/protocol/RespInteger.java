package io.codecrafters.redis.protocol;

public record RespInteger(long value) implements RespValue {
}
