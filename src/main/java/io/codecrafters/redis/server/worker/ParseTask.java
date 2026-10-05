package io.codecrafters.redis.server.worker;

import io.codecrafters.redis.protocol.RespParser;
import io.codecrafters.redis.protocol.RespValue;
import io.codecrafters.redis.server.client.ClientConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.Optional;
import java.util.Queue;

public class ParseTask implements Runnable{
    private final ClientConnection clientConnection;
    private final Selector selector;
    private final Queue<ParsedCommand> globalParsedCommandQueue;
    private static final int MAX_QUEUE_SIZE = 10_000;

    public ParseTask(ClientConnection clientConnection, Selector selector, Queue<ParsedCommand> globalParsedCommandQueue) {
        this.clientConnection = clientConnection;
        this.selector = selector;
        this.globalParsedCommandQueue = globalParsedCommandQueue;
    }

    @Override
    public void run() {
        try {
            clientConnection.ensureCapacity();
            ByteBuffer readBuff = clientConnection.getReadBuff();
            int bytesRead = clientConnection.getSocketChannel().read(readBuff);

            if(bytesRead==-1){
                clientConnection.getSelectionKey().cancel();
                clientConnection.close();
                selector.wakeup();
                return;
            }
            //Handle no data (shouldn't happen, but handle gracefully)

            if(bytesRead==0){
                clientConnection.setNeedsReEnable(true);
                selector.wakeup();
                return;
            }

            readBuff.flip(); //This is so that RespParser can parse the commands coming from client
            RespParser respParser = new RespParser();
            while(readBuff.hasRemaining()){
                Optional<RespValue> cmdOpt = respParser.parse(readBuff);
                if(cmdOpt.isEmpty()){
                    //Incomplete command... wait for more data
                    break;
                }

                RespValue command = cmdOpt.get();
                //Check if global command queue has space left
                if(globalParsedCommandQueue.size()>=MAX_QUEUE_SIZE){
                    clientConnection.setNeedsReEnable(false); //Don't re-enable OP_READ, let the parsed command remain with Client for now
                    selector.wakeup();
                    return;
                }
                globalParsedCommandQueue.offer(new ParsedCommand(clientConnection,command));
            }
            //Compact buffer for next read
            readBuff.compact();

            //Update activity for idle detection
            clientConnection.updateLastActivity();

            //Update activity for idle detection
            clientConnection.setNeedsReEnable(true);

            //Wake up selector so event loop can re-enable OP_READ
            selector.wakeup();

        } catch (IOException e) {
            System.err.println("ParseTask error: " + e.getMessage());
            clientConnection.getSelectionKey().cancel();
            clientConnection.close();
            selector.wakeup();
        }
    }
}
