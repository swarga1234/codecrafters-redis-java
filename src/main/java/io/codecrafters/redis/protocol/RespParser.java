package io.codecrafters.redis.protocol;

import io.codecrafters.redis.exception.IncompleteRespException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RespParser {

    public Optional<RespValue> parse(ByteBuffer buffer){
        buffer.mark();
        try{
            return Optional.of(parseValue(buffer));
        }catch (IncompleteRespException e){
            buffer.reset();
            return Optional.empty();
        }

    }

    private RespValue parseValue(ByteBuffer buffer){

        byte type= readByte(buffer);
        return switch (type) {
            case '+' -> new RespSimpleString(readLine(buffer));
            case '-' -> new RespError(readLine(buffer));
            case ':' -> new RespInteger(Long.parseLong(readLine(buffer)));
            case '$' -> parseBulkString(buffer);
            case '*' -> parseArray(buffer);
            default -> throw new IllegalStateException("Unknown RESP type: " + (char) type);
        };

    }

    private RespValue parseArray(ByteBuffer buffer) {
        int length = Integer.parseInt(readLine(buffer));
        List<RespValue> values = new ArrayList<>();
        for(int i=0; i<length; i++){
            values.add(parseValue(buffer));
        }
        return new RespArray(values);
    }

    private RespValue parseBulkString(ByteBuffer buffer) {
        int length = Integer.parseInt(readLine(buffer));

        if(length==-1){
            return new RespNullBulkString();
        }

        if(buffer.remaining()< length+2){
            throw new IncompleteRespException();
        }

        byte[] bytes = new byte[length];
        buffer.get(bytes);
        readByte(buffer); //reads \r
        readByte(buffer); //reads \n

        return new RespBulkString(new String(bytes, StandardCharsets.UTF_8));
    }

    private byte readByte(ByteBuffer buffer){
        if(!buffer.hasRemaining()){
            throw new IncompleteRespException();
        }
        return buffer.get();
    }
    private String readLine(ByteBuffer buffer){
        StringBuilder line = new StringBuilder();
        while(true){
            byte b = readByte(buffer);
            if(b=='\r'){
                readByte(buffer); //consume the \n too
                break;
            }
            line.append((char)b);
        }
        return line.toString();
    }
}
