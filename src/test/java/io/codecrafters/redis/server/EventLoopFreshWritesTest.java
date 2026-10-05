package io.codecrafters.redis.server;

import io.codecrafters.redis.dispatcher.CommandDispatcher;
import io.codecrafters.redis.protocol.RespArray;
import io.codecrafters.redis.protocol.RespBulkString;
import io.codecrafters.redis.server.client.ClientConnection;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventLoopFreshWritesTest {

    private static final long TIMEOUT_SECONDS = 10;

    @Test
    public void dispatchingMultipleRepliesAddsClientOnlyOnce() throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            ClientConnection client = new ClientConnection(socket);
            LinkedHashSet<ClientConnection> freshClients = new LinkedHashSet<>();
            CommandDispatcher dispatcher = new CommandDispatcher();
            RespArray ping = new RespArray(List.of(
                    new RespBulkString("PING".getBytes(StandardCharsets.UTF_8))));

            dispatcher.dispatch(client, null, ping, freshClients);
            dispatcher.dispatch(client, null, ping, freshClients);

            assertEquals(1, freshClients.size());
            assertTrue(freshClients.contains(client));
            assertEquals(14L, client.getOutstandingBytes());
        }
    }

    @Test
    public void partialFreshWriteEnablesOpWriteThenDrainsAndClearsBatch() throws Exception {
        try (Selector selector = Selector.open();
             ServerSocketChannel listener = ServerSocketChannel.open()) {
            listener.configureBlocking(false);
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            listener.register(selector, SelectionKey.OP_ACCEPT);

            EventLoop eventLoop = new EventLoop(selector, listener);
            AtomicReference<Throwable> eventLoopFailure = new AtomicReference<>();
            Thread eventLoopThread = new Thread(() -> {
                try {
                    eventLoop.run();
                } catch (Throwable failure) {
                    eventLoopFailure.set(failure);
                }
            });
            eventLoopThread.start();

            try (Socket client = new Socket()) {
                client.setReceiveBufferSize(1024);
                client.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
                client.connect((InetSocketAddress) listener.getLocalAddress());

                SelectionKey clientKey = awaitClientKey(selector);
                ClientConnection serverClient = (ClientConnection) clientKey.attachment();
                serverClient.getSocketChannel().setOption(StandardSocketOptions.SO_SNDBUF, 1024);

                byte[] payload = new byte[1024 * 1024];
                for (int index = 0; index < payload.length; index++) {
                    payload[index] = (byte) index;
                }
                writeEchoRequest(client, payload);

                await(() -> clientKey.isValid()
                                && (clientKey.interestOps() & SelectionKey.OP_WRITE) != 0,
                        "fresh partial write did not enable OP_WRITE");
                await(() -> freshClients(eventLoop).isEmpty(),
                        "fresh-write batch was not cleared after its flush");

                byte[] actual = client.getInputStream().readNBytes(echoResponseLength(payload.length));
                assertArrayEquals(echoResponse(payload), actual);
                await(() -> clientKey.isValid()
                                && (clientKey.interestOps() & SelectionKey.OP_WRITE) == 0,
                        "OP_WRITE remained enabled after the response drained");
                assertNull("event loop failed", eventLoopFailure.get());
            } finally {
                eventLoop.shutdown();
                selector.wakeup();
                eventLoopThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
                assertFalse("event loop did not stop", eventLoopThread.isAlive());
            }
        }
    }

    private static SelectionKey awaitClientKey(Selector selector) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            try {
                for (SelectionKey key : selector.keys().toArray(new SelectionKey[0])) {
                    if (key.attachment() instanceof ClientConnection) {
                        return key;
                    }
                }
            } catch (ConcurrentModificationException ignored) {
                // The event loop may be registering the accepted client concurrently.
            }
            Thread.sleep(5);
        }
        fail("event loop did not register the client");
        return null;
    }

    private static void await(BooleanSupplier condition, String failureMessage) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        fail(failureMessage);
    }

    private static Set<?> freshClients(EventLoop eventLoop) {
        try {
            Field field = EventLoop.class.getDeclaredField("clientsWithFreshWrites");
            field.setAccessible(true);
            Set<?> clients = (Set<?>) field.get(eventLoop);
            assertNotNull(clients);
            return clients;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not inspect the fresh-write batch", e);
        }
    }

    private static void writeEchoRequest(Socket client, byte[] payload) throws IOException {
        ByteArrayOutputStream request = new ByteArrayOutputStream(payload.length + 64);
        request.write(("*2\r\n$4\r\nECHO\r\n$" + payload.length + "\r\n")
                .getBytes(StandardCharsets.US_ASCII));
        request.write(payload);
        request.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        client.getOutputStream().write(request.toByteArray());
        client.getOutputStream().flush();
    }

    private static int echoResponseLength(int payloadLength) {
        return ("$" + payloadLength + "\r\n").length() + payloadLength + 2;
    }

    private static byte[] echoResponse(byte[] payload) {
        byte[] header = ("$" + payload.length + "\r\n").getBytes(StandardCharsets.US_ASCII);
        ByteBuffer response = ByteBuffer.allocate(header.length + payload.length + 2);
        response.put(header).put(payload).put((byte) '\r').put((byte) '\n');
        return response.array();
    }
}