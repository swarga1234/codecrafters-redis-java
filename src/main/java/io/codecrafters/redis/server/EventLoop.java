package io.codecrafters.redis.server;

import io.codecrafters.redis.dispatcher.ClientAcceptor;
import io.codecrafters.redis.dispatcher.CommandDispatcher;
import io.codecrafters.redis.dispatcher.ResponseWriter;
import io.codecrafters.redis.server.client.ClientConnection;
import io.codecrafters.redis.server.client.WriteQueueManager;
import io.codecrafters.redis.server.worker.ParseTask;
import io.codecrafters.redis.server.worker.ParsedCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*

    Yes, your understanding is essentially correct. A few precise corrections:

    RedisServer opens a ServerSocketChannel and binds it to a port such as 6379.
    A Selector watches the registered channels. It does not tell the serverChannel anything; it reports readiness back to the event loop.
    The event loop repeatedly asks the selector for events:
    OP_ACCEPT: a new client can connect.
    OP_READ: an existing client has data available to read.
    When OP_ACCEPT occurs:
    The server accepts the client.
    Java returns a new SocketChannel for that client.
    That client channel is registered with the selector for OP_READ.
    When OP_READ occurs:
    The event loop reads data from that specific client.
    It processes the command.
    It sends the response, currently +PONG\r\n.
    The flow is:
    RedisServer
    creates ServerSocketChannel
    creates Selector
    registers server channel for OP_ACCEPT
    starts EventLoop

    EventLoop:
    selector.select()

    OP_ACCEPT:
        accept client
        register client for OP_READ

    OP_READ:
        read client data
        process command
        send response

    repeat


 */

public class EventLoop {

    private final Selector selector;
    private final ClientAcceptor clientAcceptor;
    //private final ProtocolHandler protocolHandler;
    private final ResponseWriter responseWriter;
    private final CommandDispatcher dispatcher;

    private final Queue<ParsedCommand> globalParsedCommandQueue = new ConcurrentLinkedQueue<>();
    private final LinkedHashSet<ClientConnection> clientsWithFreshWrites = new LinkedHashSet<>();

    private static final int AVAILABLE_CPU_CORES = Runtime.getRuntime().availableProcessors();
    private static final int WORKER_THREADS = (int) Math.max(AVAILABLE_CPU_CORES, AVAILABLE_CPU_CORES*1.5);
    ExecutorService executorService = Executors.newFixedThreadPool(WORKER_THREADS);

    private volatile boolean running=true;
    private long lastIdleCheckTime = System.currentTimeMillis();
    private long lastSlowClientCheckTime = System.currentTimeMillis();
    private static final long IDLE_CHECK_INTERVAL_MS = 30_000;  // Check every 30 seconds
    private static final long IDLE_TIMEOUT_MS = 60_000;  // Close if idle > 60 seconds

    public EventLoop(Selector selector, ServerSocketChannel serverChannel) {
        this.selector = selector;
        this.dispatcher = new CommandDispatcher();
        this.clientAcceptor = new ClientAcceptor(selector, serverChannel);
        //this.protocolHandler = new ProtocolHandler(new CommandDispatcher());
        this.responseWriter= new ResponseWriter();
    }

    public void run() throws IOException {
        while (running){

            //waits until at least one registered channel is ready with 5 second timeout for idle checks.
            //System.out.println("Total worker threads: "+WORKER_THREADS);
            selector.select(5000);
            
            //Check for idle connections periodically
            if(System.currentTimeMillis() - lastIdleCheckTime > IDLE_CHECK_INTERVAL_MS) {
                closeIdleConnections(IDLE_TIMEOUT_MS);
                lastIdleCheckTime = System.currentTimeMillis();
            }
            // Check slow clients (same interval)
            if(System.currentTimeMillis() - lastSlowClientCheckTime > IDLE_CHECK_INTERVAL_MS) {
                closeSlowClients();
                lastSlowClientCheckTime = System.currentTimeMillis();
            }

            
            //returns the channels that became ready.
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            //iterate through channels that are ready
            while (keys.hasNext()){
                SelectionKey selectionKey = keys.next();
                //remove the selected key after processing otherwise  same event can be processed repeatedly
                keys.remove();

                if(selectionKey.isAcceptable()){
                    clientAcceptor.handle();
                }

                if(selectionKey.isValid() && selectionKey.isReadable()){ //one of the connected clients has sent data
                    //protocolHandler.handle(selectionKey);
                    handleReadable(selectionKey);
                }

                if(selectionKey.isValid() && selectionKey.isWritable()){
                    responseWriter.handleWrite(selectionKey);
                }
            }

             drainCommandQueue();

            // Re-enable reads for clients that have completed backpressure
            reEnableReadsForClientsAsNeeded();

            // Flush Clients with Fresh Writes before closing
            flushClientsWithFreshWrites();

            
        }
        executorService.shutdown();
    }

    private void handleReadable(SelectionKey selectionKey) {

        //get the client's stored information
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        if(clientConnection==null){
            System.err.println("No attachment for readable key");
            selectionKey.cancel();
            return;
        }

        selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_READ); //Disable read for the particular client
        executorService.submit(new ParseTask(clientConnection,selector,globalParsedCommandQueue));
    }

    private void reEnableReadsForClientsAsNeeded() {
        for(SelectionKey key : selector.keys() ){
            if(key.isValid() && key.attachment() instanceof ClientConnection clientConnection){
                if(clientConnection.isNeedsReEnable()){
                    int ops = key.interestOps();
                    key.interestOps(ops | SelectionKey.OP_READ);
                    clientConnection.setNeedsReEnable(false);
                }
            }
        }
    }

    private void drainCommandQueue() {
        while (!globalParsedCommandQueue.isEmpty()){
            ParsedCommand parsedCommand = globalParsedCommandQueue.poll();
            if(parsedCommand==null){
                throw new RuntimeException("Parsed Command can't be null!");
            }
            dispatcher.dispatch(parsedCommand.clientConnection(), parsedCommand.clientConnection().getSelectionKey(), parsedCommand.command(), clientsWithFreshWrites);
        }
    }

    private void closeIdleConnections(long timeoutMs) {
        for(SelectionKey key : selector.keys()) {
            if(key.isValid() && key.attachment() instanceof ClientConnection conn) {
                if(conn.isIdle(timeoutMs)) {
                    key.cancel();
                    conn.close();
                }
            }
        }
    }

    private void closeSlowClients(){
        long now =System.currentTimeMillis();
        for(SelectionKey selectionKey: selector.keys()){
            if(selectionKey.attachment() instanceof ClientConnection clientConnection){
                long outstandingBytes = clientConnection.getOutstandingBytes();
                long timeSinceWrite = now - clientConnection.getLastWriteTime();

                if(outstandingBytes > WriteQueueManager.MAX_QUEUED_BYTES && timeSinceWrite > 5000){
                    System.err.println("Closing slow client: " + outstandingBytes + " bytes stuck for " + timeSinceWrite + "ms");
                    selectionKey.cancel();
                    clientConnection.close();
                }
            }
        }
    }

    private void flushClientsWithFreshWrites() {
//        for(SelectionKey key : selector.keys() ){
//            if(key.isValid() && key.attachment() instanceof ClientConnection clientConnection){
//                long bytesBefore = clientConnection.getOutstandingBytes();
//                if(clientConnection.isHasFreshPendingWrites()){
//                    try {
//                        clientConnection.writePendingTo(clientConnection.getSocketChannel());
//                    } catch (IOException e) {
//                        System.err.println("Write error: " + e.getMessage());
//                        key.cancel();
//                        clientConnection.close();
//                        continue;
//                        //return;
//                    }
//                    long bytesAfter = clientConnection.getOutstandingBytes();
//                    if(bytesAfter<bytesBefore){
//                        clientConnection.updateLastWriteTime();
//                    }
//                    if(clientConnection.hasPendingWrites()){
//                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
//                    }else {
//                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE );
//                    }
//                    clientConnection.setHasFreshPendingWrites(false);
//                }
//            }
//        }
        for (ClientConnection clientConnection : clientsWithFreshWrites) {
            long bytesBefore = clientConnection.getOutstandingBytes();
            SelectionKey key = clientConnection.getSelectionKey();
            if (clientConnection.isHasFreshPendingWrites()) {
                try {
                    clientConnection.writePendingTo(clientConnection.getSocketChannel());
                } catch (IOException e) {
                    System.err.println("Write error: " + e.getMessage());
                    key.cancel();
                    clientConnection.close();
                    continue;
                    //return;
                }
                long bytesAfter = clientConnection.getOutstandingBytes();
                if (bytesAfter < bytesBefore) {
                    clientConnection.updateLastWriteTime();
                }
                if (clientConnection.hasPendingWrites()) {
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                } else {
                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                }
                clientConnection.setHasFreshPendingWrites(false);
            }
        }
        clientsWithFreshWrites.clear();
    }

    public void shutdown(){
        running=false;
    }


}
