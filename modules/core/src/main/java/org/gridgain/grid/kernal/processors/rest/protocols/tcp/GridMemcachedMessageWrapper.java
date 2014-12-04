/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridMemcachedMessage.*;

/**
 * Memcached message wrapper for direct marshalling.
 */
public class GridMemcachedMessageWrapper extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 3053626103006980626L;

    /** UTF-8 charset. */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Memcached message bytes.
     */
    private byte[] bytes;

    /**
     *
     */
    public GridMemcachedMessageWrapper() {
        // No-op.
    }

    /**
     * @param msg Message.
     * @param jdkMarshaller JDK marshaller.
     * @throws GridException If failed to marshal.
     */
    public GridMemcachedMessageWrapper(GridMemcachedMessage msg, GridMarshaller jdkMarshaller) throws GridException {
        bytes = encodeMemcache(msg, jdkMarshaller);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArrayClient(bytes))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return MEMCACHE_RES_FLAG;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridMemcachedMessageWrapper _clone = new GridMemcachedMessageWrapper();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridMemcachedMessageWrapper _clone = (GridMemcachedMessageWrapper)_msg;

        _clone.bytes = bytes;
    }

    /**
     * Encodes memcache message to a raw byte array.
     *
     * @param msg Message being serialized.
     * @param jdkMarshaller JDK marshaller.
     * @return Serialized message.
     * @throws org.gridgain.grid.GridException If serialization failed.
     */
    private byte[] encodeMemcache(GridMemcachedMessage msg, GridMarshaller jdkMarshaller) throws GridException {
        GridByteArrayList res = new GridByteArrayList(HDR_LEN - 1);

        int keyLen = 0;

        int keyFlags = 0;

        if (msg.key() != null) {
            ByteArrayOutputStream rawKey = new ByteArrayOutputStream();

            keyFlags = encodeObj(msg.key(), rawKey, jdkMarshaller);

            msg.key(rawKey.toByteArray());

            keyLen = rawKey.size();
        }

        int dataLen = 0;

        int valFlags = 0;

        if (msg.value() != null) {
            ByteArrayOutputStream rawVal = new ByteArrayOutputStream();

            valFlags = encodeObj(msg.value(), rawVal, jdkMarshaller);

            msg.value(rawVal.toByteArray());

            dataLen = rawVal.size();
        }

        int flagsLen = 0;

        if (msg.addFlags())
            flagsLen = FLAGS_LENGTH;

        res.add(msg.operationCode());

        // Cast is required due to packet layout.
        res.add((short)keyLen);

        // Cast is required due to packet layout.
        res.add((byte)flagsLen);

        // Data type is always 0x00.
        res.add((byte)0x00);

        res.add((short)msg.status());

        res.add(keyLen + flagsLen + dataLen);

        res.add(msg.opaque(), 0, msg.opaque().length);

        // CAS, unused.
        res.add(0L);

        assert res.size() == HDR_LEN - 1;

        if (flagsLen > 0) {
            res.add((short) keyFlags);
            res.add((short) valFlags);
        }

        assert msg.key() == null || msg.key() instanceof byte[];
        assert msg.value() == null || msg.value() instanceof byte[];

        if (keyLen > 0)
            res.add((byte[])msg.key(), 0, ((byte[])msg.key()).length);

        if (dataLen > 0)
            res.add((byte[])msg.value(), 0, ((byte[])msg.value()).length);

        return res.entireArray();
    }

    /**
     * Encodes given object to a byte array and returns flags that describe the type of serialized object.
     *
     * @param obj Object to serialize.
     * @param out Output stream to which object should be written.
     * @param jdkMarshaller JDK marshaller.
     * @return Serialization flags.
     * @throws GridException If JDK serialization failed.
     */
    private int encodeObj(Object obj, ByteArrayOutputStream out, GridMarshaller jdkMarshaller) throws GridException {
        int flags = 0;

        byte[] data = null;

        if (obj instanceof String)
            data = ((String)obj).getBytes(UTF_8);
        else if (obj instanceof Boolean) {
            data = new byte[] {(byte)((Boolean)obj ? '1' : '0')};

            flags |= BOOLEAN_FLAG;
        }
        else if (obj instanceof Integer) {
            data = U.intToBytes((Integer) obj);

            flags |= INT_FLAG;
        }
        else if (obj instanceof Long) {
            data = U.longToBytes((Long)obj);

            flags |= LONG_FLAG;
        }
        else if (obj instanceof Date) {
            data = U.longToBytes(((Date)obj).getTime());

            flags |= DATE_FLAG;
        }
        else if (obj instanceof Byte) {
            data = new byte[] {(Byte)obj};

            flags |= BYTE_FLAG;
        }
        else if (obj instanceof Float) {
            data = U.intToBytes(Float.floatToIntBits((Float)obj));

            flags |= FLOAT_FLAG;
        }
        else if (obj instanceof Double) {
            data = U.longToBytes(Double.doubleToLongBits((Double)obj));

            flags |= DOUBLE_FLAG;
        }
        else if (obj instanceof byte[]) {
            data = (byte[])obj;

            flags |= BYTE_ARR_FLAG;
        }
        else {
            jdkMarshaller.marshal(obj, out);

            flags |= SERIALIZED_FLAG;
        }

        if (data != null)
            out.write(data, 0, data.length);

        return flags;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMemcachedMessageWrapper.class, this);
    }
}
