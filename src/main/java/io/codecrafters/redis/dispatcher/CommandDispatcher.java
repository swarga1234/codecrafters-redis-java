package io.codecrafters.redis.dispatcher;

import io.codecrafters.redis.factory.CommandRegistry;
import io.codecrafters.redis.protocol.RespArray;
import io.codecrafters.redis.protocol.RespValue;
import io.codecrafters.redis.protocol.RespWriter;
import io.codecrafters.redis.rediscommand.TypicalRedisCommand;
import io.codecrafters.redis.server.ClientConnection;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;

public class CommandDispatcher {

    public void dispatch(ClientConnection clientConnection, SelectionKey selectionKey, RespValue command) {

        if(command instanceof RespArray arr){
            List<RespValue> commandList = arr.values();
            if(commandList.isEmpty()){
                return;
            }
            String cmdName = commandList.getFirst().getStringValue().toUpperCase();
            try {
                TypicalRedisCommand redisCommand = CommandRegistry.getCommand(cmdName);
                RespValue response = redisCommand.execute(commandList.subList(1, commandList.size()));
                sendResponse(clientConnection,selectionKey, response);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void sendResponse(ClientConnection clientConnection, SelectionKey selectionKey, RespValue resp) {
        ByteBuffer buffer = RespWriter.encode(resp);
        if(!clientConnection.enqueueWrite(buffer)){
            System.err.println("Backpressure: closing client");
            selectionKey.cancel();
            clientConnection.close();
            return;
        }

        int oldOps = selectionKey.interestOps();
        selectionKey.interestOps(oldOps | SelectionKey.OP_WRITE);

    }
}
