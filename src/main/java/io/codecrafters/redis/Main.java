package io.codecrafters.redis;

import io.codecrafters.redis.protocol.RespArray;
import io.codecrafters.redis.protocol.RespBulkString;
import io.codecrafters.redis.protocol.RespParser;
import io.codecrafters.redis.protocol.RespValue;
import io.codecrafters.redis.server.RedisServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
        new RedisServer(6379).start();
    //  Uncomment the code below to pass the first stage
//        ServerSocket serverSocket = null;
//        Socket clientSocket = null;
//        int port = 6379;
//        String response = "+PONG\r\n";
//        try {
//          serverSocket = new ServerSocket(port);
//          // Since the tester restarts your program quite often, setting SO_REUSEADDR
//          // ensures that we don't run into 'Address already in use' errors
//          serverSocket.setReuseAddress(true);
//          // Wait for connection from client.
//          clientSocket = serverSocket.accept();
//          InputStream inputStream = clientSocket.getInputStream();
//          byte[] buffer = new byte[1024];
//          while(inputStream.read(buffer)!=-1){
//              clientSocket.getOutputStream().write(response.getBytes());
//          }
//
//        } catch (IOException e) {
//          System.out.println("IOException: " + e.getMessage());
//        } finally {
//          try {
//            if (clientSocket != null) {
//              clientSocket.close();
//            }
//          } catch (IOException e) {
//            System.out.println("IOException: " + e.getMessage());
//          }
//        }
      //String msg = "*2\r\n$4\r\nECHO\r\n$3\r\nHEY\r\n";
//      String msg="$-1\r\n";
//      ByteBuffer buffer = ByteBuffer.allocate(1024);
//      buffer.put(msg.getBytes(StandardCharsets.UTF_8));
//      buffer.flip(); // <-- important: switch to read mode before parsing
//
//      RespParser parser = new RespParser();
//      Optional<RespValue> opt = parser.parse(buffer);
//
//      if (opt.isEmpty()) {
//          System.out.println("Incomplete data");
//          return;
//      }
//
//      RespValue value = opt.get();
//      System.out.println(value);
  }
}
