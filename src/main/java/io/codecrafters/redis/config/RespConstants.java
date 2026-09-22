package io.codecrafters.redis.config;

public class RespConstants {

    //RESP2
    public final static String SIMPLE_STRING_PREFIX = "+";
    public final static String SIMPLE_ERROR_PREFIX="-";
    public final static String INTEGER_PREFIX=":";
    public final static String BULK_STRING_PREFIX="$";
    public final static String NULL_BULK_STRING_PREFIX="$-1\r\n";
    public final static String ARRAY_PREFIX="*";

    //RESP3
    public final static String NULL_PREFIX="_";
    public final static String BOOLEAN_PREFIX="#";
    public final static String DOUBLE_PREFIX=",";
    public final static String BIG_NUMBER_PREFIX="(";
    public final static String BULK_ERROR_PREFIX = "!";
    public final static String VERBATIM_STRING_PREFIX = "=";
    public final static String MAP_PREFIX = "%";
    public final static String ATTRIBUTE_PREFIX = "|";
    public final static String SET_PREFIX = "~";
    public final static String PUSH_PREFIX = ">";

}
