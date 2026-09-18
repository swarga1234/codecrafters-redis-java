package io.codecrafters.redis;

import io.codecrafters.redis.server.RedisServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

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
  }
}
