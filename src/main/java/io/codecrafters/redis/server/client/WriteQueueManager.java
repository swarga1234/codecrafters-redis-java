package io.codecrafters.redis.server.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Deque;

public class WriteQueueManager {
    private final Deque<ByteBuffer> writeQueue = new ArrayDeque<>();
    private static final int MAX_QUEUED_WRITES = 10000;
    public static final long MAX_QUEUED_BYTES = 10 * 1024 * 1024;  // 10 MB
    private static final int MAX_BYTES_PER_WRITE_TURN = 64 * 1024; //64 KB
    private long cachedOutstandingBytes = 0;

    public boolean enqueueWrite(ByteBuffer buffer){
        //adding response to be sent to the client
        //buffer is in read mode hence remaining indicates how many bytes left to be read in the buffer

        if(writeQueue.size()>=MAX_QUEUED_WRITES) {
            return false;
        }
        int bytes = buffer.remaining();
        // if(bytes > MAX_QUEUED_BYTES - cachedOutstandingBytes){
        //     return false;
        // }
        if(bytes==0){
            return false;
        }
        writeQueue.addLast(buffer);
        cachedOutstandingBytes+=bytes;
        return true;
    }

    // Writing to the socket channel from write queue.
    public void writePendingTo(WritableByteChannel channel) throws IOException {
        int writtenThisTurn = 0;
        while (hasPendingWrites() && writtenThisTurn < MAX_BYTES_PER_WRITE_TURN){
            //gets the first element without removing.
            ByteBuffer head = writeQueue.peekFirst();
            if(head==null){
                break;
            }
            // int before = head.remaining();
            //Tries to write to the channel.

            int remainingBudget = MAX_BYTES_PER_WRITE_TURN - writtenThisTurn;
            int bytesAllowed = Math.min(head.remaining(), remainingBudget);
            int originalLimit = head.limit();
            int bytesWritten=0;
            try{
                head.limit(head.position()+bytesAllowed);
                bytesWritten=channel.write(head);
            }finally {
                head.limit(originalLimit);
            }

            //int bytesWritten =
            if(bytesWritten==0){
                break;
            }
            writtenThisTurn+=bytesWritten;
            cachedOutstandingBytes-=bytesWritten;
            //If the buffer was completely written then hasRemain should return false and pointer should move to next buffer. Else the loop breaks
            if(!head.hasRemaining()){
                writeQueue.removeFirst();
            }
        }
    }

    public boolean tryWriteAndQueue(SocketChannel channel, ByteBuffer byteBuffer) throws IOException {

        int bytesWritten = channel.write(byteBuffer);

        // if fully written no need to queue
        if(!byteBuffer.hasRemaining()){
            return true;
        }
        return enqueueWrite(byteBuffer);
    }
    public boolean hasPendingWrites(){
        return !writeQueue.isEmpty();
    }

    public void clear() {
        writeQueue.clear();
        cachedOutstandingBytes = 0;
    }

    public long getOutstandingBytes() {
        return cachedOutstandingBytes;
    }
}
