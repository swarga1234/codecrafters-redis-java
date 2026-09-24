package io.codecrafters.redis.protocol;

public record RespBulkString(String value) implements RespValue {

    @Override
    public String getStringValue() {
        return value();
    }
}
