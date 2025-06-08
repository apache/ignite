package org.apache.ignite.internal;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public abstract class AbstractMessage implements Message {
    @Order(0)
    private int id;

    public int id() {
        return id;
    }

    public void id(int id) {
        this.id = id;
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
