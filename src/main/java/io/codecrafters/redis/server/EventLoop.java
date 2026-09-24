package io.codecrafters.redis.server;

import io.codecrafters.redis.factory.CommandRegistry;
import io.codecrafters.redis.protocol.*;
import io.codecrafters.redis.rediscommand.TypicalRedisCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

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
    private final ServerSocketChannel serverChannel;

    public EventLoop(Selector selector, ServerSocketChannel serverChannel) {
        this.selector = selector;
        this.serverChannel = serverChannel;
    }

    public void run() throws IOException {
        while (true){

            //waits until at least one registered channel is ready. It blocks efficiently, so the CPU is not constantly busy checking.
            selector.select();
            //returns the channels that became ready.
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            //iterate through channels that are ready
            while (keys.hasNext()){
                SelectionKey selectionKey = keys.next();
                //remove the selected key after processing otherwise  same event can be processed repeatedly
                keys.remove();

                if(selectionKey.isAcceptable()){
                    acceptClient();
                }

                if(selectionKey.isValid() && selectionKey.isReadable()){ //one of the connected clients has sent data
                    readClient(selectionKey);
                }

                if(selectionKey.isValid() && selectionKey.isWritable()){
                    handleWrite(selectionKey);
                }
            }


        }
    }

    private void handleWrite(SelectionKey selectionKey) {
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        SocketChannel socket = clientConnection.getSocketChannel();

        try {
            clientConnection.writePendingTo(socket);
        } catch (IOException e) {
            System.err.println("Write error: " + e.getMessage());
            selectionKey.cancel();
            clientConnection.close();
            return;
        }

        if (!clientConnection.hasPendingWrites()){
            int ops= selectionKey.interestOps();
            selectionKey.interestOps(ops & ~SelectionKey.OP_WRITE);
        }
    }

    private void readClient(SelectionKey selectionKey) throws IOException {

        //get the client's stored information
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        SocketChannel client = clientConnection.getSocketChannel();
        ByteBuffer readBuff = clientConnection.getReadBuff();
        int bytesRead= client.read(readBuff);
        if(bytesRead ==-1){
            selectionKey.cancel();
            clientConnection.close();
            return;
        }
        if(bytesRead==0){
            return;
        }
        readBuff.flip(); //This is so that RespParser can parse the commands coming from client
        RespParser respParser = new RespParser();
        while (readBuff.hasRemaining()){
           Optional<RespValue> cmdOpt= respParser.parse(readBuff);
           if(cmdOpt.isEmpty()){
                break;
           }
           RespValue command = cmdOpt.get();
           handleCommand(clientConnection, selectionKey, command);
        }
        readBuff.compact();
    }

    private void handleCommand(ClientConnection clientConnection, SelectionKey selectionKey, RespValue command) {

        if(command instanceof RespArray arr){
            List<RespValue> commandList = arr.values();
            if(commandList.isEmpty()){
                return;
            }
//            String cmdName = extractString(commandList.getFirst()).toUpperCase();
//
//            switch (cmdName){
//                case "PING":
//                    RespValue reponse = new RespSimpleString("PONG");
//                    sendResponse(clientConnection, selectionKey, reponse);
//                    break;
//                case "ECHO":
//                    if (commandList.size() > 1) {
//                        RespValue echo = commandList.get(1);
//                        sendResponse(clientConnection, selectionKey, echo);
//                    }
//                    break;
//            }
            String cmdName = commandList.getFirst().getStringValue().toUpperCase();
            try {
                TypicalRedisCommand redisCommand = CommandRegistry.getCommand(cmdName);
                RespValue response = redisCommand.execute(commandList.subList(1, commandList.size()));
                sendResponse(clientConnection,selectionKey, response);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void sendResponse(ClientConnection clientConnection, SelectionKey selectionKey, RespValue resp) {
        ByteBuffer buffer = RespWriter.encode(resp);
        if(!clientConnection.enqueueWrite(buffer)){
            System.err.println("Backpressure: closing client");
            selectionKey.cancel();
            clientConnection.close();
            return;
        }

        int oldOps = selectionKey.interestOps();
        selectionKey.interestOps(oldOps | SelectionKey.OP_WRITE);

    }


    private void acceptClient() throws IOException {
        SocketChannel client = serverChannel.accept();
        if(client!=null){
            client.configureBlocking(false);
            ClientConnection clientConnection = new ClientConnection(client);
            client.register(selector, SelectionKey.OP_READ, clientConnection); //Selector, watch this client and tell me whenever it sends data.
            //SocketChannel + OP_READ ---> watches for data from an existing client
        }
    }

}
