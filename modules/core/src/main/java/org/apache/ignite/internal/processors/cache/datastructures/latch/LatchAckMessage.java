package org.apache.ignite.internal.processors.cache.datastructures.latch;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Ack message is sent to coordinator when {@link Latch#countDown()} is invoked.
 */
public class LatchAckMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    private String latchId;

    private AffinityTopologyVersion topVer;

    private boolean isFinal;

    public LatchAckMessage(String latchId, AffinityTopologyVersion topVer, boolean isFinal) {
        this.latchId = latchId;
        this.topVer = topVer;
        this.isFinal = isFinal;
    }

    public LatchAckMessage() {
    }

    public String latchId() {
        return latchId;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeBoolean("isFinal", isFinal))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("latchId", latchId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                isFinal = reader.readBoolean("isFinal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                latchId = reader.readString("latchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(LatchAckMessage.class);
    }

    @Override
    public short directType() {
        return 130;
    }

    @Override
    public byte fieldsCount() {
        return 3;
    }

    @Override
    public void onAckReceived() {
        // No-op.
    }
}
