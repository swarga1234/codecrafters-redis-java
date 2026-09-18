package io.codecrafters.redis.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

public class RedisServer {

    private final int port;

    public RedisServer(int port) {
        this.port = port;
    }

    public void start() {

        try (Selector selector = Selector.open();
                ServerSocketChannel serverSocketChannel =  ServerSocketChannel.open()){
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(port));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT); //Selector, watch this server channel and tell me whenever a new client wants to connect.
            EventLoop eventLoop = new EventLoop(selector, serverSocketChannel);
            eventLoop.run();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

    }
}
