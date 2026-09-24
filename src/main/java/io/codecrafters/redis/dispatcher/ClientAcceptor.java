package io.codecrafters.redis.dispatcher;

import io.codecrafters.redis.server.ClientConnection;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class ClientAcceptor {

    private final Selector selector;
    private final ServerSocketChannel serverChannel;

    public ClientAcceptor(Selector selector, ServerSocketChannel serverChannel) {
        this.selector = selector;
        this.serverChannel = serverChannel;
    }

    public void handle() throws IOException {
        SocketChannel client = serverChannel.accept();
        if(client!=null){
            client.configureBlocking(false);
            ClientConnection clientConnection = new ClientConnection(client);
            client.register(selector, SelectionKey.OP_READ, clientConnection); //Selector, watch this client and tell me whenever it sends data.
            //SocketChannel + OP_READ ---> watches for data from an existing client
        }
    }
}
