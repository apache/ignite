package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.value.*;

import java.nio.*;

/**
 * H2 Decimal.
 */
public class GridH2Decimal extends GridH2ValueMessage {
    /** */
    private byte[] b;

    /**
     *
     */
    public GridH2Decimal() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Decimal(Value val) {
        assert val.getType() == Value.DECIMAL : val.getType();

        b = val.getBytesNoCopy();
    }

    /** {@inheritDoc} */
    @Override public Value value() {
        return ValueBytes.getNoCopy(b).convertTo(Value.DECIMAL);
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
                if (!writer.writeByteArray("b", b))
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
                b = reader.readByteArray("b");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -10;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
