/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.h2.engine.SysProperties;
import org.h2.util.Bits;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Implementation of the BINARY data type.
 * It is also the base class for ValueJavaObject.
 */
public class ValueBytes extends Value {

    /**
     * Empty value.
     */
    public static final ValueBytes EMPTY = new ValueBytes(Utils.EMPTY_BYTES);

    /**
     * The value.
     */
    protected byte[] value;

    /**
     * Associated TypeInfo.
     */
    protected TypeInfo type;

    /**
     * The hash code.
     */
    protected int hash;

    protected ValueBytes(byte[] v) {
        this.value = v;
    }

    /**
     * Get or create a bytes value for the given byte array.
     * Clone the data.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueBytes get(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        b = Utils.cloneByteArray(b);
        return getNoCopy(b);
    }

    /**
     * Get or create a bytes value for the given byte array.
     * Do not clone the date.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueBytes getNoCopy(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        ValueBytes obj = new ValueBytes(b);
        if (b.length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueBytes) Value.cache(obj);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            long precision = value.length;
            this.type = type = new TypeInfo(BYTES, precision, 0, MathUtils.convertLongToInt(precision * 2), null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return BYTES;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append("X'");
        return StringUtils.convertBytesToHex(builder, getBytesNoCopy()).append('\'');
    }

    @Override
    public byte[] getBytesNoCopy() {
        return value;
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(getBytesNoCopy());
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        byte[] v2 = ((ValueBytes) v).value;
        if (mode.isBinaryUnsigned()) {
            return Bits.compareNotNullUnsigned(value, v2);
        }
        return Bits.compareNotNullSigned(value, v2);
    }

    @Override
    public String getString() {
        return StringUtils.convertBytesToHex(value);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = Utils.getByteArrayHash(value);
        }
        return hash;
    }

    @Override
    public Object getObject() {
        return getBytes();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setBytes(parameterIndex, value);
    }

    @Override
    public int getMemory() {
        return value.length + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueBytes
                && Arrays.equals(value, ((ValueBytes) other).value);
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (value.length <= precision) {
            return this;
        }
        int len = MathUtils.convertLongToInt(precision);
        return getNoCopy(Arrays.copyOf(value, len));
    }

}
