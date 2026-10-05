package io.codecrafters.redis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteTest {

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        System.out.printf("after allocate: pos=%d limit=%d cap=%d%n", buffer.position(), buffer.limit(), buffer.capacity());

        buffer.put("hello".getBytes(StandardCharsets.UTF_8));
        //System.out.println(buffer.toString());
        System.out.printf("after put \"hello\": pos=%d limit=%d remaining=%d capacity=%d%n", buffer.position(), buffer.limit(), buffer.remaining(), buffer.capacity());

        buffer.flip();
        System.out.printf("after flip: pos=%d limit=%d remaining=%d%n", buffer.position(), buffer.limit(), buffer.remaining());

        byte first = buffer.get();
        System.out.printf("read one byte: '%c'  pos=%d remaining=%d%n", (char) first, buffer.position(), buffer.remaining());
//
//        buffer.compact(); // preserve unread bytes ("ello") and prepare for writing after them
//        System.out.printf("after compact: pos=%d limit=%d remaining=%d%n", buffer.position(), buffer.limit(), buffer.remaining());

        buffer.put("X".getBytes(StandardCharsets.UTF_8)); // append more data
        System.out.printf("after put \"X\": pos=%d limit=%d remaining=%d%n", buffer.position(), buffer.limit(), buffer.remaining());

        buffer.flip(); // prepare to read preserved + newly written bytes
        System.out.printf("after flip2: pos=%d limit=%d remaining=%d%n", buffer.position(), buffer.limit(), buffer.remaining());

        byte[] all = new byte[buffer.remaining()];
        buffer.get(all);
        System.out.println("read all bytes: \"" + new String(all, StandardCharsets.UTF_8) + "\"");

        buffer.clear();
        System.out.printf("after clear: pos=%d limit=%d remaining=%d%n", buffer.position(), buffer.limit(), buffer.remaining());
    }
}
