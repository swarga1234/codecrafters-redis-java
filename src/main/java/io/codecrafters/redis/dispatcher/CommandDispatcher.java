package io.codecrafters.redis.dispatcher;

import io.codecrafters.redis.factory.CommandRegistry;
import io.codecrafters.redis.protocol.*;
import io.codecrafters.redis.rediscommand.TypicalRedisCommand;
import io.codecrafters.redis.server.client.ClientConnection;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.LinkedHashSet;
import java.util.List;

public class CommandDispatcher {

    public void dispatch(ClientConnection clientConnection, SelectionKey selectionKey, RespValue command, LinkedHashSet<ClientConnection> clientsWithFreshWrites) {

        if(command instanceof RespArray arr){
            List<RespValue> commandList = arr.values();
            if(commandList.isEmpty()){
                return;
            }
            RespValue cmdResp = commandList.getFirst();
            if(!(cmdResp instanceof RespBulkString commandArg)){
                sendResponse(clientConnection, selectionKey, new RespError("ERR command name must be a bulk string"), clientsWithFreshWrites);
                return;
            }
            String cmdName = commandArg.getStringValue();
            if(cmdName==null || cmdName.isBlank())
            {
                sendResponse(clientConnection, selectionKey, new RespError("Empty Command"), clientsWithFreshWrites);
                return;
            }

            try {
                TypicalRedisCommand redisCommand = CommandRegistry.getCommand(cmdName.toUpperCase());
                if(redisCommand.getMinArgs()>commandList.size()-1){
                    sendResponse(clientConnection, selectionKey, new RespError("Too few arguments for command: "+ redisCommand), clientsWithFreshWrites);
                    return;
                }
                RespValue response = redisCommand.execute(commandList.subList(1, commandList.size()));
                sendResponse(clientConnection,selectionKey, response, clientsWithFreshWrites);
            } catch (Exception e) {
                sendResponse(clientConnection, selectionKey, new RespError(e.getMessage()), clientsWithFreshWrites);
            }
        }

    }

    private void sendResponse(ClientConnection clientConnection, SelectionKey selectionKey, RespValue resp, LinkedHashSet<ClientConnection> clientsWithFreshWrites) {
        ByteBuffer buffer = RespWriter.encode(resp);
        if(!clientConnection.enqueueWrite(buffer)){
            System.err.println("Delayed Client response");
            return;
        }
        // clientConnection.updateLastWriteTime();
        clientConnection.setHasFreshPendingWrites(true);
        clientsWithFreshWrites.add(clientConnection);

    }
}
