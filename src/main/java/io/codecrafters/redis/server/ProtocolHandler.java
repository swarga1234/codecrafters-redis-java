package io.codecrafters.redis.server;

import io.codecrafters.redis.dispatcher.CommandDispatcher;
import io.codecrafters.redis.protocol.RespParser;
import io.codecrafters.redis.protocol.RespValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;

public class ProtocolHandler {

    private final CommandDispatcher dispatcher;

    public ProtocolHandler(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void handle(SelectionKey selectionKey) throws IOException {

        //get the client's stored information
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        SocketChannel client = clientConnection.getSocketChannel();
        ByteBuffer readBuff = clientConnection.getReadBuff();
        int bytesRead= client.read(readBuff);
        if(bytesRead ==-1){
            selectionKey.cancel();
            clientConnection.close();
            return;
        }
        if(bytesRead==0){
            return;
        }
        readBuff.flip(); //This is so that RespParser can parse the commands coming from client
        RespParser respParser = new RespParser();
        while (readBuff.hasRemaining()){
            Optional<RespValue> cmdOpt= respParser.parse(readBuff);
            if(cmdOpt.isEmpty()){
                break;
            }
            RespValue command = cmdOpt.get();
            dispatcher.dispatch(clientConnection, selectionKey, command);
        }
        readBuff.compact();
    }

}
