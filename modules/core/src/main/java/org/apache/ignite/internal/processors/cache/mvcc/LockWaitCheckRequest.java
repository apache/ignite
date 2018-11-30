package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class LockWaitCheckRequest implements MvccMessage {
    private static final long serialVersionUID = 0;

    private UUID futId;
    private GridCacheVersion txVersion;

    public LockWaitCheckRequest() {
    }

    public LockWaitCheckRequest(UUID id, GridCacheVersion txVersion) {
        futId = id;
        this.txVersion = txVersion;
    }

    public UUID futId() {
        return futId;
    }

    public GridCacheVersion txVersion() {
        return txVersion;
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
                if (!writer.writeUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("txVersion", txVersion))
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
                futId = reader.readUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                txVersion = reader.readMessage("txVersion");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(LockWaitCheckRequest.class);
    }

    @Override public short directType() {
        return 168;
    }

    @Override public byte fieldsCount() {
        return 2;
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
