package io.codecrafters.redis.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

public class RedisServer {

    private final int port;
    private volatile EventLoop eventLoop;
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
            eventLoop = new EventLoop(selector, serverSocketChannel);
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
            System.out.println("Redis server started on port " + port);
            eventLoop.run();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

    }

    public void stop(){
        if(eventLoop!=null){
            System.out.println("Shutdown signal received");
            eventLoop.shutdown();
        }
    }
}
