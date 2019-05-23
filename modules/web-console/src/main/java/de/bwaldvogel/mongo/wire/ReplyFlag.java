package de.bwaldvogel.mongo.wire;

public enum ReplyFlag {
    CURSOR_NOT_FOUND(0),
    QUERY_FAILURE(1),
    SHARD_CONFIG_STALE(2),
    AWAIT_CAPABLE(3);

    private int value;

    ReplyFlag(int bit) {
        this.value = 1 << bit;
    }

    public int addTo(int flags) {
        return flags | value;
    }
}
