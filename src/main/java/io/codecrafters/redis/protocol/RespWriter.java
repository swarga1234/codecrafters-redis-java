package io.codecrafters.redis.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class RespWriter {

    private RespWriter(){

    }

    public static ByteBuffer encode(RespValue respValue){
        return switch (respValue){
            case RespSimpleString respSimpleString -> getRespSimpleString(respSimpleString.value());
            case RespBulkString respBulkString -> getRespBulkString(respBulkString.value());
            case RespError respError -> getRespError(respError.message());
            case RespInteger respInteger -> getRespInteger(respInteger.value());
            case RespNullBulkString respNullBulkString -> getRespNullBulkString();
            case RespArray respArray -> getRespArray(respArray.values());
            default -> throw new IllegalStateException("Unexpected value: " + respValue);
        };
    }

    public static ByteBuffer getRespArray(List<RespValue> values) {
        if(values==null){
            return ByteBuffer.wrap(("*-1\r\n").getBytes(StandardCharsets.UTF_8));
        }
        byte[] header = ("*"+values.size()+"\r\n").getBytes(StandardCharsets.UTF_8);
        int total=0;
        List<ByteBuffer> encodedBuffers = new ArrayList<>();
        for(RespValue respValue: values){
            ByteBuffer encodedBuffer = encode(respValue);
            encodedBuffers.add(encodedBuffer);
            total+=encodedBuffer.remaining();
        }

        ByteBuffer buffer = ByteBuffer.allocate(header.length+total);
        buffer.put(header);
        for(ByteBuffer encodedBuffer: encodedBuffers){
            ByteBuffer duplicate = encodedBuffer.asReadOnlyBuffer();
            buffer.put(duplicate);
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getRespNullBulkString() {
        byte[] bytes= ("$-1\r\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getRespInteger(long value) {
        byte[] bytes= (":"+value+"\r\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getRespError(String message) {
        byte[] bytes= ("-"+message+"\r\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getRespBulkString(String value) {
        if(value==null){
            return getRespNullBulkString();
        }
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        byte[] header = ("$"+data.length+"\r\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(header.length+data.length+2);
        buffer.put(header);
        buffer.put(data);
        buffer.put((byte) '\r');
        buffer.put((byte) '\n');
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getRespSimpleString(String value) {
        byte[] bytes = ("+"+value+"\r\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }
}
