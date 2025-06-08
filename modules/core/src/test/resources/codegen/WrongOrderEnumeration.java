package org.apache.ignite.internal;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class WrongOrderEnumeration implements Message {
    @Order(0)
    public int id;

    @Order(2)
    public String str;

    public int id() {
        return id;
    }

    public void id(int id) {
        this.id = id;
    }

    public int str() {
        return str;
    }

    public void str(String str) {
        this.str = str;
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
