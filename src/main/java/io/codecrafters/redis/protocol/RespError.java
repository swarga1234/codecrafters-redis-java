package io.codecrafters.redis.protocol;

public record RespError(String message) implements RespValue {
}
