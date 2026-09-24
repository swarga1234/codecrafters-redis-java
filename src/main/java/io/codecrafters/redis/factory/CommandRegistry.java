package io.codecrafters.redis.factory;

import io.codecrafters.redis.rediscommand.EchoCommand;
import io.codecrafters.redis.rediscommand.PingCommand;
import io.codecrafters.redis.rediscommand.TypicalRedisCommand;

import java.util.HashMap;
import java.util.Map;

public class CommandRegistry {

    private static final Map<String, Class<? extends TypicalRedisCommand>> COMMANDS = new HashMap<>();
    static {
        COMMANDS.put("PING", PingCommand.class);
        COMMANDS.put("ECHO", EchoCommand.class);
        //add more classes for commands
    }

    public static TypicalRedisCommand getCommand(String commandName) throws Exception {
        Class<? extends TypicalRedisCommand> clazz = COMMANDS.get(commandName.toUpperCase());
        if(clazz == null){
            throw new Exception("Unknown command: " + commandName);
        }
        return clazz.getDeclaredConstructor().newInstance();

    }
}
