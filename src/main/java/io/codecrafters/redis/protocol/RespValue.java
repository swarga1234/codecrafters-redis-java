package io.codecrafters.redis.protocol;

public sealed interface RespValue permits RespSimpleString, RespError, RespInteger, RespBulkString, RespNullBulkString, RespArray {
}
