package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class DeadlockProbe implements MvccMessage {
    private static final long serialVersionUID = 0;

    private GridCacheVersion initiatorVersion;
    private GridCacheVersion waitingVersion;
    private GridCacheVersion blockerVersion;

    public DeadlockProbe() {
    }

    public DeadlockProbe(GridCacheVersion initiatorVersion, GridCacheVersion waitingVersion,
        GridCacheVersion blockerVersion) {
        this.initiatorVersion = initiatorVersion;
        this.waitingVersion = waitingVersion;
        this.blockerVersion = blockerVersion;
    }

    public GridCacheVersion initiatorVersion() {
        return initiatorVersion;
    }

    public GridCacheVersion waitingVersion() {
        // t0d0 why do we need that version?
        return waitingVersion;
    }

    public GridCacheVersion blockerVersion() {
        return blockerVersion;
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
                if (!writer.writeMessage("blockerVersion", blockerVersion))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("initiatorVersion", initiatorVersion))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("waitingVersion", waitingVersion))
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
                blockerVersion = reader.readMessage("blockerVersion");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                initiatorVersion = reader.readMessage("initiatorVersion");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                waitingVersion = reader.readMessage("waitingVersion");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(DeadlockProbe.class);
    }

    @Override public short directType() {
        return 167;
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
