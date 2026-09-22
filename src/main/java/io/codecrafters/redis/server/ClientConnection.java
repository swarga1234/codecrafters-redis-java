package io.codecrafters.redis.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

public final class ClientConnection {

    private final SocketChannel socketChannel;
    private ByteBuffer readBuff = ByteBuffer.allocate(1024);
    private final Deque<ByteBuffer> writeQueue = new ArrayDeque<>();
    private final int maxQueuedWrites = 64;
    private final int maxQueuedBytes = 64 * 1024; // total no of bytes the write queue can hold

    public ClientConnection(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public ByteBuffer getReadBuff() {
        return readBuff;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    //
    public void replaceReadBuffer(ByteBuffer newBuff){
        this.readBuff=newBuff;
    }

    public boolean enqueueWrite(ByteBuffer buffer){
        //adding response to be sent to the client
        //buffer is in read mode hence remaining indicates how many bytes left to be read in the buffer
        int bytes = buffer.remaining();
        if(outstandingBytes()+bytes>maxQueuedBytes){
            return false;
        }
        if(writeQueue.size()>=maxQueuedWrites) {
            return false;
        }
        writeQueue.addLast(buffer);
        return true;
    }

    public int outstandingBytes(){
        // Total bytes waiting to be written
        int total=0;
        for(ByteBuffer buffer : writeQueue){
            total+=buffer.remaining();
        }
        return total;
    }

    public ByteBuffer peekPendingWrite() {
        return  writeQueue.peekFirst();
    }

    public ByteBuffer pollPendingWrite(){
        return  writeQueue.pollFirst();
    }

    public boolean hasPendingWrites(){
        return !writeQueue.isEmpty();
    }

    // Writing to the socket channel from write queue.
    public void writePendingTo(SocketChannel channel) throws IOException {
        while (hasPendingWrites()){
            //gets the first element without removing.
            ByteBuffer head = writeQueue.peekFirst();
            if(head==null){
                break;
            }
            //Tries to write to the channel.
            channel.write(head);
            //If the buffer was completely written then hasRemain should return false and pointer should move to next buffer. Else the loop breaks
            if(head.hasRemaining()){
                break;
            }
            writeQueue.removeFirst();
        }
    }

    public void close() {
        try { socketChannel.close(); } catch (IOException ignored) {}
        writeQueue.clear();
    }
}
