package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.value.*;

import java.lang.reflect.*;
import java.nio.*;

/**
 * H2 Geometry.
 */
public class GridH2Geometry extends GridH2ValueMessage {
    /** */
    private static final Method GEOMETRY_FROM_BYTES;

    /**
     * Initialize field.
     */
    static {
        try {
            GEOMETRY_FROM_BYTES = Class.forName("org.h2.value.ValueGeometry").getMethod("get", byte[].class);
        }
        catch (NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalStateException("Check H2 version in classpath.");
        }
    }

    /** */
    private byte[] b;

    /**
     *
     */
    public GridH2Geometry() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Geometry(Value val) {
        assert val.getType() == Value.GEOMETRY : val.getType();

        b = val.getBytesNoCopy();
    }

    /** {@inheritDoc} */
    @Override public Value value() {
        try {
            return (Value)GEOMETRY_FROM_BYTES.invoke(null, new Object[]{b});
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
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
        return -21;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
