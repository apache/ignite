package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class PartitionsExchangeFinishedCheckResponse implements Message {

    private AffinityTopologyVersion topVer;

    private boolean finished;

    public PartitionsExchangeFinishedCheckResponse() {
    }

    public PartitionsExchangeFinishedCheckResponse(
        AffinityTopologyVersion topVer, boolean finished) {
        this.topVer = topVer;
        this.finished = finished;
    }

    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    public boolean finished() {
        return finished;
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
                if (!writer.writeBoolean("finished", finished))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("topVer", topVer))
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
                finished = reader.readBoolean("finished");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(PartitionsExchangeFinishedCheckResponse.class);
    }

    @Override public short directType() {
        return 137;
    }

    @Override public byte fieldsCount() {
        return 2;
    }

    @Override public void onAckReceived() {
        // No-op
    }
}
