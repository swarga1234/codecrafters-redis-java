package io.codecrafters.redis.server.client;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriteQueueManagerTest {

    @Test
    public void preservesQueuedBytesAcrossPartialAndZeroWrites() throws IOException {
        WriteQueueManager manager = new WriteQueueManager();
        ByteBuffer response = ByteBuffer.wrap("abcdefghij".getBytes());
        ScriptedChannel channel = new ScriptedChannel(3, 0, Integer.MAX_VALUE);

        assertTrue(manager.enqueueWrite(response));
        assertEquals(10L, manager.getOutstandingBytes());

        manager.writePendingTo(channel);

        assertEquals(3, response.position());
        assertEquals(7L, manager.getOutstandingBytes());
        assertTrue(manager.hasPendingWrites());
        assertEquals("abc", channel.output());

        manager.writePendingTo(channel);

        assertEquals(10, response.position());
        assertEquals(0L, manager.getOutstandingBytes());
        assertFalse(manager.hasPendingWrites());
        assertEquals("abcdefghij", channel.output());
    }

    private static final class ScriptedChannel implements WritableByteChannel {
        private final Deque<Integer> writeLimits = new ArrayDeque<>();
        private final ByteArrayOutputStream writtenBytes = new ByteArrayOutputStream();
        private boolean open = true;

        private ScriptedChannel(int... writeLimits) {
            for (int writeLimit : writeLimits) {
                this.writeLimits.addLast(writeLimit);
            }
        }

        @Override
        public int write(ByteBuffer source) {
            int limit = writeLimits.removeFirst();
            int bytesToWrite = Math.min(limit, source.remaining());
            byte[] bytes = new byte[bytesToWrite];
            source.get(bytes);
            writtenBytes.writeBytes(bytes);
            return bytesToWrite;
        }

        private String output() {
            return writtenBytes.toString();
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}