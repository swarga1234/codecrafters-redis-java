package io.codecrafters.redis.protocol;

public record RespSimpleString(String value) implements RespValue {
}
