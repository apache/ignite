package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class ActivationMessageResponse implements Message {
    /** Node id. */
    private UUID nodeId;

    /** Ex message. */
    private String exMsg;

    /**
     * Default constructor.
     */
    public ActivationMessageResponse() {
         /* No-op. */
    }

    /**
     * @param nodeId Node id.
     * @param exMsg Ex message.
     */
    public ActivationMessageResponse(UUID nodeId, String exMsg) {
        this.nodeId = nodeId;
        this.exMsg = exMsg;
    }

    /**
     *
     */
    public UUID getNodeId() {
        return nodeId;
    }
    /**
     *
     */
    public String getExceptionMsg() {
        return exMsg;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeString("exMsg", exMsg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                exMsg = reader.readString("exMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ActivationMessageResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 127;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }
}
