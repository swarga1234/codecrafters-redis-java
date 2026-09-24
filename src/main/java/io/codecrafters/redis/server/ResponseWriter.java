package io.codecrafters.redis.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ResponseWriter {

    public void handleWrite(SelectionKey selectionKey) {
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        SocketChannel socket = clientConnection.getSocketChannel();

        try {
            clientConnection.writePendingTo(socket);
        } catch (IOException e) {
            System.err.println("Write error: " + e.getMessage());
            selectionKey.cancel();
            clientConnection.close();
            return;
        }

        if (!clientConnection.hasPendingWrites()){
            int ops= selectionKey.interestOps();
            selectionKey.interestOps(ops & ~SelectionKey.OP_WRITE);
        }
    }
}
