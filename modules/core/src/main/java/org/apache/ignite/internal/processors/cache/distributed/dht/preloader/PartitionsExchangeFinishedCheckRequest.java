package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class PartitionsExchangeFinishedCheckRequest implements Message {

    private AffinityTopologyVersion topVer;

    public PartitionsExchangeFinishedCheckRequest() {
    }

    public PartitionsExchangeFinishedCheckRequest(
        AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    public AffinityTopologyVersion topVer() {
        return topVer;
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
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(PartitionsExchangeFinishedCheckRequest.class);
    }

    @Override public short directType() {
        return 136;
    }

    @Override public byte fieldsCount() {
        return 1;
    }

    @Override public void onAckReceived() {
        // No-op
    }
}
