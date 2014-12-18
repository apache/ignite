/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.apache.ignite.portables.*;
import org.jetbrains.annotations.*;

import java.math.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Portable writer implementation.
 */
public class GridTcpCommunicationPortableWriter implements PortableWriter {
    /** Stream. */
    private final GridTcpCommunicationPortableStream stream;

    /**
     * @param stream Stream.
     */
    public GridTcpCommunicationPortableWriter(GridTcpCommunicationPortableStream stream) {
        this.stream = stream;
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws PortableException {
        stream.writeByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws PortableException {
        stream.writeShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws PortableException {
        stream.writeInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws PortableException {
        stream.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws PortableException {
        stream.writeFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws PortableException {
        stream.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws PortableException {
        stream.writeChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws PortableException {
        stream.writeBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws PortableException {
        stream.writeInt(val != null ? val.length : -1);

        if (val != null)
            stream.writeBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws PortableException {
        if (obj != null) {
            assert obj instanceof GridTcpCommunicationMessageAdapter;

            stream.writeMessage((GridTcpCommunicationMessageAdapter)obj);
        }
        else
            stream.writeByte(Byte.MIN_VALUE);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public PortableRawWriter rawWriter() {
        throw new UnsupportedOperationException();
    }
}
