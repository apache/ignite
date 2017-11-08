package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class WalModeDynamicChangeFinishedMessage extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID uid;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public WalModeDynamicChangeFinishedMessage() {
        // No-op.
    }

    /**
     * @param uid Uid.
     */
    public WalModeDynamicChangeFinishedMessage(UUID uid) {
        this.uid = uid;
    }

    /**
     *
     */
    public UUID uid() {
        return uid;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 2:
                if (!writer.writeUuid("uid", uid))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 2:
                uid = reader.readUuid("uid");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(WalModeDynamicChangeFinishedMessage.class);
    }
}
