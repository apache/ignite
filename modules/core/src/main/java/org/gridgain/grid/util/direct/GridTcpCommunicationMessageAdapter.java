/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Communication message adapter.
 */
public abstract class GridTcpCommunicationMessageAdapter implements Serializable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected static final Object NULL = new Object();

    /** */
    protected final GridTcpCommunicationMessageState commState = new GridTcpCommunicationMessageState();

    /**
     * @param writer Writer.
     */
    public final void setWriter(MessageWriter writer) {
        assert writer != null;

        commState.setWriter(writer);
    }

    /**
     * @param reader Reader.
     */
    public final void setReader(MessageReader reader) {
        assert reader != null;

        commState.setReader(reader);
    }

    /**
     * @param buf Byte buffer.
     * @return Whether message was fully written.
     */
    public abstract boolean writeTo(ByteBuffer buf);

    /**
     * @param buf Byte buffer.
     * @return Whether message was fully read.
     */
    public abstract boolean readFrom(ByteBuffer buf);

    /**
     * @return Message type.
     */
    public abstract byte directType();

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override public abstract GridTcpCommunicationMessageAdapter clone();

    /**
     * Clones all fields of the provided message to {@code this}.
     *
     * @param _msg Message to clone from.
     */
    protected abstract void clone0(GridTcpCommunicationMessageAdapter _msg);

    /**
     * @return {@code True} if should skip recovery for this message.
     */
    public boolean skipRecovery() {
        return false;
    }

    /**
     * @param arr Array.
     * @return Array iterator.
     */
    protected final Iterator<?> arrayIterator(final Object[] arr) {
        return new Iterator<Object>() {
            private int idx;

            @Override public boolean hasNext() {
                return idx < arr.length;
            }

            @Override public Object next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return arr[idx++];
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
