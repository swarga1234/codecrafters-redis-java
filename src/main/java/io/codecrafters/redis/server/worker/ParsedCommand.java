package io.codecrafters.redis.server.worker;

import io.codecrafters.redis.protocol.RespValue;
import io.codecrafters.redis.server.client.ClientConnection;

public record ParsedCommand(ClientConnection clientConnection, RespValue command) {
    public ParsedCommand {
        if(clientConnection==null || command==null){
            throw new IllegalArgumentException("clientConnection and command cannot be null");
        }
    }
}
