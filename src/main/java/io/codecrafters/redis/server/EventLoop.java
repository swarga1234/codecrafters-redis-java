package io.codecrafters.redis.server;

import io.codecrafters.redis.dispatcher.CommandDispatcher;
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
    private final ClientAcceptor clientAcceptor;
    private final ProtocolHandler protocolHandler;
    private final ResponseWriter responseWriter;

    public EventLoop(Selector selector, ServerSocketChannel serverChannel) {
        this.selector = selector;
        this.clientAcceptor = new ClientAcceptor(selector, serverChannel);
        this.protocolHandler = new ProtocolHandler(new CommandDispatcher());
        this.responseWriter= new ResponseWriter();
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
                    clientAcceptor.handle();
                }

                if(selectionKey.isValid() && selectionKey.isReadable()){ //one of the connected clients has sent data
                    protocolHandler.handle(selectionKey);
                }

                if(selectionKey.isValid() && selectionKey.isWritable()){
                    responseWriter.handleWrite(selectionKey);
                }
            }


        }
    }


}
