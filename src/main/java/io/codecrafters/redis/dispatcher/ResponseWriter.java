package io.codecrafters.redis.dispatcher;

import io.codecrafters.redis.server.client.ClientConnection;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ResponseWriter {

    public void handleWrite(SelectionKey selectionKey) {
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        if(clientConnection==null){
            System.err.println("No attachment for readable key");
            selectionKey.cancel();
            return;
        }
        SocketChannel socket = clientConnection.getSocketChannel();
        long bytesBefore = clientConnection.getOutstandingBytes();
        try {
            clientConnection.writePendingTo(socket);
        } catch (IOException e) {
            System.err.println("Write error: " + e.getMessage());
            selectionKey.cancel();
            clientConnection.close();
            return;
        }
        clientConnection.setHasFreshPendingWrites(false);
        long bytesAfter = clientConnection.getOutstandingBytes();
        if(bytesAfter<bytesBefore){
            clientConnection.updateLastWriteTime();
        }

        if (!clientConnection.hasPendingWrites()){
            int ops= selectionKey.interestOps();
            selectionKey.interestOps(ops & ~SelectionKey.OP_WRITE);
        }
    }
}
