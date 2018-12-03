package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class LockWaitCheckRequest implements MvccMessage {
    private static final long serialVersionUID = 0;

    private IgniteUuid futId;
    private GridCacheVersion txVersion;

    public LockWaitCheckRequest() {
    }

    public LockWaitCheckRequest(IgniteUuid futId, GridCacheVersion txVersion) {
        this.futId = futId;
        this.txVersion = txVersion;
    }

    public IgniteUuid futId() {
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
                if (!writer.writeIgniteUuid("futId", futId))
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
                futId = reader.readIgniteUuid("futId");

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
