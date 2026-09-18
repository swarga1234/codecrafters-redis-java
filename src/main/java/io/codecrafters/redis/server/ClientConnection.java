package io.codecrafters.redis.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ClientConnection {

    private final SocketChannel socketChannel;
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

    public ClientConnection(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }
}
