package io.codecrafters.redis.server.client;

public class ClientState {

    private long lastActivityTime = System.currentTimeMillis();
    private long lastWriteTime = System.currentTimeMillis();
    private volatile boolean needsReEnable=false;
    private volatile boolean hasFreshPendingWrites;

    public boolean isHasFreshPendingWrites() {
        return hasFreshPendingWrites;
    }

    public void setHasFreshPendingWrites(boolean hasFreshPendingWrites) {
        this.hasFreshPendingWrites = hasFreshPendingWrites;
    }

    public boolean isNeedsReEnable() {
        return needsReEnable;
    }

    public void setNeedsReEnable(boolean needsReEnable) {
        this.needsReEnable = needsReEnable;
    }

    public void updateLastActivity() {
        this.lastActivityTime = System.currentTimeMillis();
    }

    public boolean isIdle(long timeoutMs) {
        return System.currentTimeMillis() - lastActivityTime > timeoutMs;
    }

    public void updateLastWriteTime() {
        this.lastWriteTime = System.currentTimeMillis();
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }
}
