package io.codecrafters.redis.protocol;

public record RespError(String message) implements RespValue {

    @Override
    public String getStringValue() {
        return message();
    }
}
