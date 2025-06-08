package org.apache.ignite.internal;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

public class MatrixMessageMessage implements Message {
    @Order(0)
    private GridCacheVersion[][] ver;

    public GridCacheVersion[][] ver() {
        return ver;
    }

    public void ver(GridCacheVersion[][] ver) {
        this.ver = ver;
    }

    public short directType() {
        return 0;
    }

    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return true;
    }

    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }

    public void onAckReceived() {
        // No-op.
    }
}