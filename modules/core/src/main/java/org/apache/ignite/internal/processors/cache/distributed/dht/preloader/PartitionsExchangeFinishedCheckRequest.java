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

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        return reader.afterMessageRead(PartitionsExchangeFinishedCheckRequest.class);
    }

    @Override public short directType() {
        return 136;
    }

    @Override public byte fieldsCount() {
        return 0;
    }

    @Override public void onAckReceived() {
        // No-op
    }
}
