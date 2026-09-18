package io.codecrafters.redis.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

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

                if(selectionKey.isReadable()){
                    readClient(selectionKey);
                }
            }


        }
    }

    private void readClient(SelectionKey selectionKey) throws IOException {
        ClientConnection clientConnection = (ClientConnection) selectionKey.attachment();
        SocketChannel client = clientConnection.getSocketChannel();
        ByteBuffer byteBuffer = clientConnection.getByteBuffer();
        int bytesRead= client.read(byteBuffer);
        if(bytesRead ==-1){
            selectionKey.cancel();
            client.close();
            return;
        }
        if(bytesRead>0){
            client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
            byteBuffer.clear();
        }
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
