package io.codecrafters.redis.protocol;

import java.nio.charset.StandardCharsets;

public record RespBulkString(byte[] value) implements RespValue {
    @Override
    public String getStringValue() {
        return new String(value, StandardCharsets.UTF_8);
    }
}
