package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class LockWaitCheckResponse implements MvccMessage {
    private static final long serialVersionUID = 0;

    private UUID futId;
    private GridCacheVersion blockerTxVersion;
    private UUID blockerNodeId;

    public LockWaitCheckResponse() {
    }

    public LockWaitCheckResponse(UUID futId, GridCacheVersion blockerTxVersion, UUID blockerNodeId) {
        this.futId = futId;
        this.blockerTxVersion = blockerTxVersion;
        this.blockerNodeId = blockerNodeId;
    }

    public UUID futId() {
        return futId;
    }

    public GridCacheVersion blockerTxVersion() {
        return blockerTxVersion;
    }

    public UUID blockerNodeId() {
        return blockerNodeId;
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeUuid("blockerNodeId", blockerNodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("blockerTxVersion", blockerTxVersion))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("futId", futId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                blockerNodeId = reader.readUuid("blockerNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                blockerTxVersion = reader.readMessage("blockerTxVersion");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                futId = reader.readUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(LockWaitCheckResponse.class);
    }

    @Override public short directType() {
        return 169;
    }

    @Override public byte fieldsCount() {
        return 3;
    }

    @Override public void onAckReceived() {

    }

    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    @Override public boolean processedFromNioThread() {
        return false;
    }
}
