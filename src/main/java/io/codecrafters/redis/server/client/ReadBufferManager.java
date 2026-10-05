package io.codecrafters.redis.server.client;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ReadBufferManager {

    private ByteBuffer buffer;
    private static final int INITIAL_SIZE=1024;
    private static final long MAX_SIZE = 512 * 1024 * 1024;  // 512MB

    public ReadBufferManager() {
        this.buffer = ByteBuffer.allocate(INITIAL_SIZE);
    }

    public int getCurrentSize(){
        return buffer.capacity();
    }
    public void compact(){
        buffer.compact();
    }

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    public void ensureCapacity() throws IOException {
        if(buffer.remaining()==0){
            int newCapacity = buffer.capacity()*2;
            if(newCapacity>MAX_SIZE){
                throw new IOException("Command exceeds maximum size: 512MB");
            }
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
            buffer.flip();
            newBuffer.put(buffer);
            this.buffer=newBuffer;
        }
    }
}
