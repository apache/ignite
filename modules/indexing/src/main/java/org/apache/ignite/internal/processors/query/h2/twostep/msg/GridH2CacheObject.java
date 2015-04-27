package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.h2.opt.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.value.*;

import java.nio.*;

/**
 * H2 Cache object message.
 */
public class GridH2CacheObject extends GridH2ValueMessage {
    /** */
    private int cacheId;

    /** */
    private CacheObject obj;

    /**
     *
     */
    public GridH2CacheObject() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param obj Object.
     */
    public GridH2CacheObject(int cacheId, CacheObject obj) {
        this.cacheId = cacheId;
        this.obj = obj;
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return new GridH2ValueCacheObject(ctx.cache().context().cacheContext(cacheId), obj);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 0:
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                obj = reader.readMessage("obj");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
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
            case 0:
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("obj", obj))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -22;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }
}
