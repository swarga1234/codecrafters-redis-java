package io.codecrafters.redis.server.worker;

import io.codecrafters.redis.protocol.RespArray;
import io.codecrafters.redis.protocol.RespBulkString;
import io.codecrafters.redis.protocol.RespValue;
import io.codecrafters.redis.server.client.ClientConnection;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParseTaskReadPathTest {

    @Test
    public void retainsIncompleteRespUntilRemainingBytesArrive() throws IOException {
        try (SocketPair sockets = SocketPair.open(); Selector selector = Selector.open()) {
            ClientConnection client = new ClientConnection(sockets.server);
            Queue<ParsedCommand> commands = new ConcurrentLinkedQueue<>();
            byte[] firstFragment = "*1\r\n$4\r\nPI".getBytes(StandardCharsets.US_ASCII);
            byte[] secondFragment = "NG\r\n".getBytes(StandardCharsets.US_ASCII);

            writeFully(sockets.client, firstFragment);
            parseOnce(client, selector, commands);

            assertTrue(commands.isEmpty());
            assertEquals(firstFragment.length, client.getReadBuff().position());
            assertTrue(client.isNeedsReEnable());

            writeFully(sockets.client, secondFragment);
            parseOnce(client, selector, commands);

            ParsedCommand parsed = commands.poll();
            assertEquals(client, parsed.clientConnection());
            assertEquals("PING", commandName(parsed.command()));
            assertTrue(commands.isEmpty());
            assertEquals(0, client.getReadBuff().position());
        }
    }

    @Test
    public void parsesPipelinedCommandsInOrder() throws IOException {
        try (SocketPair sockets = SocketPair.open(); Selector selector = Selector.open()) {
            ClientConnection client = new ClientConnection(sockets.server);
            Queue<ParsedCommand> commands = new ConcurrentLinkedQueue<>();
            byte[] pipeline = ("*1\r\n$4\r\nPING\r\n"
                    + "*1\r\n$4\r\nPING\r\n"
                    + "*1\r\n$4\r\nPING\r\n").getBytes(StandardCharsets.US_ASCII);
            writeFully(sockets.client, pipeline);

            for (int attempt = 0; attempt < 3 && commands.size() < 3; attempt++) {
                parseOnce(client, selector, commands);
            }

            assertEquals(3, commands.size());
            for (int index = 0; index < 3; index++) {
                ParsedCommand parsed = commands.poll();
                assertEquals(client, parsed.clientConnection());
                assertEquals("PING", commandName(parsed.command()));
            }
            assertTrue(commands.isEmpty());
        }
    }

    @Test
    public void expandsReadBufferForBulkCommandLargerThanInitialCapacity() throws IOException {
        try (SocketPair sockets = SocketPair.open(); Selector selector = Selector.open()) {
            ClientConnection client = new ClientConnection(sockets.server);
            Queue<ParsedCommand> commands = new ConcurrentLinkedQueue<>();
            byte[] payload = new byte[8192];
            for (int index = 0; index < payload.length; index++) {
                payload[index] = (byte) index;
            }

            byte[] header = ("*2\r\n$4\r\nECHO\r\n$" + payload.length + "\r\n")
                    .getBytes(StandardCharsets.US_ASCII);
            writeFully(sockets.client, ByteBuffer.wrap(header));
            writeFully(sockets.client, ByteBuffer.wrap(payload));
            writeFully(sockets.client, ByteBuffer.wrap("\r\n".getBytes(StandardCharsets.US_ASCII)));

            for (int attempt = 0; attempt < 8 && commands.isEmpty(); attempt++) {
                parseOnce(client, selector, commands);
            }

            ParsedCommand parsed = commands.poll();
            assertTrue("bulk command was not parsed", parsed != null);
            assertTrue("read buffer did not grow", client.getReadBuff().capacity() > 1024);
            assertEquals("ECHO", commandName(parsed.command()));
            RespArray command = (RespArray) parsed.command();
            assertArrayEquals(payload, ((RespBulkString) command.values().get(1)).value());
            assertTrue(commands.isEmpty());
        }
    }

    @Test
    public void zeroByteNonblockingReadRequestsReadReenableWithoutCommand() throws IOException {
        try (SocketPair sockets = SocketPair.open(); Selector selector = Selector.open()) {
            sockets.server.configureBlocking(false);
            ClientConnection client = new ClientConnection(sockets.server);
            Queue<ParsedCommand> commands = new ConcurrentLinkedQueue<>();

            parseOnce(client, selector, commands);

            assertTrue(commands.isEmpty());
            assertTrue(client.isNeedsReEnable());
            assertEquals(0, client.getReadBuff().position());
        }
    }

    @Test
    public void preservesParsedCommandUntilQueueHasCapacity() throws Exception {
        try (SocketPair sockets = SocketPair.open(); Selector selector = Selector.open()) {
            ClientConnection client = new ClientConnection(sockets.server);
            ArrayBlockingQueue<ParsedCommand> commands = new ArrayBlockingQueue<>(1);
            RespArray ping = new RespArray(List.of(
                    new RespBulkString("PING".getBytes(StandardCharsets.UTF_8))));
            commands.add(new ParsedCommand(client, ping));
            writeFully(sockets.client, "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));

            Thread task = new Thread(new ParseTask(client, selector, commands));
            task.start();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
            while (task.isAlive() && task.getState() != Thread.State.WAITING
                    && System.nanoTime() < deadline) {
                Thread.yield();
            }

            assertTrue("parsed command was discarded instead of waiting for capacity", task.isAlive());
            assertEquals(Thread.State.WAITING, task.getState());
            assertEquals(1, commands.size());

            commands.poll();
            task.join(2000);

            assertFalse("parse task did not resume after capacity became available", task.isAlive());
            ParsedCommand parsed = commands.poll();
            assertTrue("parsed command was lost", parsed != null);
            assertEquals(client, parsed.clientConnection());
            assertEquals("PING", commandName(parsed.command()));
            assertTrue(client.isNeedsReEnable());
            assertEquals(0, client.getReadBuff().position());
        }
    }

    private static void parseOnce(ClientConnection client, Selector selector,
                                  Queue<ParsedCommand> commands) {
        new ParseTask(client, selector, commands).run();
    }

    private static String commandName(RespValue value) {
        assertTrue(value instanceof RespArray);
        RespArray command = (RespArray) value;
        assertFalse(command.values().isEmpty());
        assertTrue(command.values().getFirst() instanceof RespBulkString);
        return ((RespBulkString) command.values().getFirst()).getStringValue();
    }

    private static void writeFully(SocketChannel channel, byte[] bytes) throws IOException {
        writeFully(channel, ByteBuffer.wrap(bytes));
    }

    private static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static final class SocketPair implements AutoCloseable {
        private final SocketChannel client;
        private final SocketChannel server;

        private SocketPair(SocketChannel client, SocketChannel server) {
            this.client = client;
            this.server = server;
        }

        private static SocketPair open() throws IOException {
            try (ServerSocketChannel listener = ServerSocketChannel.open()) {
                listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                SocketChannel client = SocketChannel.open();
                try {
                    client.connect((InetSocketAddress) listener.getLocalAddress());
                    return new SocketPair(client, listener.accept());
                } catch (IOException e) {
                    client.close();
                    throw e;
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                client.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                server.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}