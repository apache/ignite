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
 * Portable reader implementation.
 */
public class GridTcpCommunicationPortableReader implements PortableReader {
    /** Stream. */
    private final GridTcpCommunicationPortableStream stream;

    /**
     * @param stream Stream.
     */
    public GridTcpCommunicationPortableReader(GridTcpCommunicationPortableStream stream) {
        this.stream = stream;
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws PortableException {
        return stream.readByte();
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws PortableException {
        return stream.readShort();
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws PortableException {
        return stream.readInt();
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws PortableException {
        return stream.readLong();
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws PortableException {
        return stream.readFloat();
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws PortableException {
        return stream.readDouble();
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws PortableException {
        return stream.readChar();
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws PortableException {
        return stream.readBoolean();
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws PortableException {
        return stream.readByteArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws PortableException {
        return stream.readShortArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws PortableException {
        return stream.readIntArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws PortableException {
        return stream.readLongArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws PortableException {
        return stream.readFloatArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws PortableException {
        return stream.readDoubleArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws PortableException {
        return stream.readCharArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws PortableException {
        return stream.readBooleanArray(stream.readInt());
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T readObject(String fieldName) throws PortableException {
        return (T)stream.readMessage();
    }

    /** {@inheritDoc} */
    @Nullable @Override public BigDecimal readDecimal(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public BigDecimal[] readDecimalArray(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName, Class<? extends Collection<T>> colCls)
        throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName, Class<T> enumCls)
        throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray(String fieldName, Class<T> enumCls)
        throws PortableException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public PortableRawReader rawReader() {
        throw new UnsupportedOperationException();
    }
}
