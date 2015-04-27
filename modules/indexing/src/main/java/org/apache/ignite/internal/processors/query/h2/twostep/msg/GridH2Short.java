package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.value.*;

import java.nio.*;

/**
 * H2 Short.
 */
public class GridH2Short extends GridH2ValueMessage {
    /** */
    private short x;

    /**
     *
     */
    public GridH2Short() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Short(Value val) {
        assert val.getType() == Value.SHORT : val.getType();

        x = val.getShort();
    }

    /** {@inheritDoc} */
    @Override public Value value() {
        return ValueShort.get(x);
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
                if (!writer.writeShort("x", x))
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
            case 0:
                x = reader.readShort("x");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -7;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
