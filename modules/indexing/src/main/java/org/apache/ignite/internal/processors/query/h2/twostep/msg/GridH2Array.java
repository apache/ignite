package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.value.*;

import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.*;

/**
 * H2 Array.
 */
public class GridH2Array extends GridH2ValueMessage {
    /** */
    @GridDirectCollection(Message.class)
    private Collection<Message> x;

    /**
     *
     */
    public GridH2Array() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Array(Value val) {
        assert val.getType() == Value.ARRAY : val.getType();

        ValueArray arr = (ValueArray)val;

        x = new ArrayList<>(arr.getList().length);

        for (Value v : arr.getList())
            x.add(toMessage(v));
    }

    /** {@inheritDoc} */
    @Override public Value value() {
        // TODO we need cache object context
        return ValueArray.get(fillArray(x.iterator(), new Value[x.size()], null));
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
                if (!writer.writeCollection("x", x, MessageCollectionItemType.MSG))
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
                x = reader.readCollection("x", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -18;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
