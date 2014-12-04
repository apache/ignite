/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.communication;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 *
 */
class GridTestMessage extends GridTcpCommunicationMessageAdapter implements Externalizable {
    /** */
    private IgniteUuid id;

    /** */
    private long field1;

    /** */
    private long field2;

    /** */
    private String str;

    /** */
    private byte[] bytes;

    /**
     * @param id Message ID.
     * @param str String.
     */
    GridTestMessage(IgniteUuid id, String str) {
        this.id = id;
        this.str = str;
    }

    /**
     * @param id Message ID.
     * @param bytes Bytes.
     */
    GridTestMessage(IgniteUuid id, byte[] bytes) {
        this.id = id;
        this.bytes = bytes;
    }

    /**
     * For Externalizable support.
     */
    public GridTestMessage() {
        // No-op.
    }

    /**
     * @return Message ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @return Bytes.
     */
    public byte[] bytes() {
        return bytes;
    }

    /**
     * @param bytes Bytes.
     */
    public void bytes(byte[] bytes) {
        this.bytes = bytes;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        out.writeLong(field1);
        out.writeLong(field2);
        U.writeString(out, str);
        U.writeByteArray(out, bytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        field1 = in.readLong();
        field2 = in.readLong();
        str = U.readString(in);
        bytes = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override public GridTcpCommunicationMessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 0;
    }
}
