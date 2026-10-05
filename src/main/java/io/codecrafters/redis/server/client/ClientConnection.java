package io.codecrafters.redis.server.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public final class ClientConnection {

    private final SocketChannel socketChannel;
    private final ClientState clientState;
    private final ReadBufferManager readBufferManager;
    private final WriteQueueManager writeQueueManager;
    private SelectionKey selectionKey;
    private volatile boolean needsReEnable=false;



    public ClientConnection(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        this.clientState = new ClientState();
        this.readBufferManager = new ReadBufferManager();
        this.writeQueueManager = new WriteQueueManager();
    }

    public ByteBuffer getReadBuff() {
        return readBufferManager.getBuffer();
    }

    public void ensureCapacity() throws IOException {
        readBufferManager.ensureCapacity();
    }

    public void compactReadBuffer() {
        readBufferManager.compact();
    }

    public boolean enqueueWrite(ByteBuffer buffer) {
        return writeQueueManager.enqueueWrite(buffer);
    }

    public boolean tryWriteAndQueue(SocketChannel channel, ByteBuffer byteBuffer) throws IOException {
        return writeQueueManager.tryWriteAndQueue(channel,byteBuffer);
    }

    public void writePendingTo(SocketChannel channel) throws IOException {
        writeQueueManager.writePendingTo(channel);
    }

    public boolean hasPendingWrites() {
        return writeQueueManager.hasPendingWrites();
    }

    public long getOutstandingBytes() {
        return writeQueueManager.getOutstandingBytes();
    }

    // State delegation
    public void updateLastActivity() {
        clientState.updateLastActivity();
    }

    public boolean isIdle(long timeoutMs) {
        return clientState.isIdle(timeoutMs);
    }

    public void updateLastWriteTime() {
        clientState.updateLastWriteTime();
    }

    public long getLastWriteTime() {
        return clientState.getLastWriteTime();
    }

    public SocketChannel getSocketChannel() {
        if(this.socketChannel==null){
            throw new RuntimeException("Socket channel is null");
        }
        return socketChannel;
    }

    public void close() {
        try { socketChannel.close(); } catch (IOException ignored) {}
        writeQueueManager.clear();
    }

    public boolean isNeedsReEnable() {
        return needsReEnable;
    }

    public void setNeedsReEnable(boolean needsReEnable) {
        this.needsReEnable = needsReEnable;
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public boolean isHasFreshPendingWrites() {
        return clientState.isHasFreshPendingWrites();
    }

    public void setHasFreshPendingWrites(boolean hasFreshPendingWrites) {
        clientState.setHasFreshPendingWrites(hasFreshPendingWrites);
    }

}
