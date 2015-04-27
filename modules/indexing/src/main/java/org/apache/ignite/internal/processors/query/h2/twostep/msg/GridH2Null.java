package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.value.*;

import java.nio.*;

/**
 * Message for {@link Value#NULL}.
 */
public class GridH2Null extends GridH2ValueMessage {
    /** */
    public static GridH2Null INSTANCE = new GridH2Null();

    /**
     * Disallow new instance creation.
     */
    private GridH2Null() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Value value() {
        return ValueNull.INSTANCE;
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

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -4;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }
}
