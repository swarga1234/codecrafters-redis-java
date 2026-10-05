package io.codecrafters.redis.datastore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisDataStore {

    public static final Map<String,String> dataStore = new ConcurrentHashMap<>();

}
