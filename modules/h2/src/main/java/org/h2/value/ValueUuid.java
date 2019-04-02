/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the UUID data type.
 */
public class ValueUuid extends Value {

    /**
     * The precision of this value in number of bytes.
     */
    static final int PRECISION = 16;

    /**
     * The display size of the textual representation of a UUID.
     * Example: cd38d882-7ada-4589-b5fb-7da0ca559d9a
     */
    static final int DISPLAY_SIZE = 36;

    private final long high, low;

    private ValueUuid(long high, long low) {
        this.high = high;
        this.low = low;
    }

    @Override
    public int hashCode() {
        return (int) ((high >>> 32) ^ high ^ (low >>> 32) ^ low);
    }

    /**
     * Create a new UUID using the pseudo random number generator.
     *
     * @return the new UUID
     */
    public static ValueUuid getNewRandom() {
        long high = MathUtils.secureRandomLong();
        long low = MathUtils.secureRandomLong();
        // version 4 (random)
        high = (high & (~0xf000L)) | 0x4000L;
        // variant (Leach-Salz)
        low = (low & 0x3fff_ffff_ffff_ffffL) | 0x8000_0000_0000_0000L;
        return new ValueUuid(high, low);
    }

    /**
     * Get or create a UUID for the given 16 bytes.
     *
     * @param binary the byte array (must be at least 16 bytes long)
     * @return the UUID
     */
    public static ValueUuid get(byte[] binary) {
        if (binary.length < 16) {
            return get(StringUtils.convertBytesToHex(binary));
        }
        long high = Bits.readLong(binary, 0);
        long low = Bits.readLong(binary, 8);
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    /**
     * Get or create a UUID for the given high and low order values.
     *
     * @param high the most significant bits
     * @param low the least significant bits
     * @return the UUID
     */
    public static ValueUuid get(long high, long low) {
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    /**
     * Get or create a UUID for the given Java UUID.
     *
     * @param uuid Java UUID
     * @return the UUID
     */
    public static ValueUuid get(UUID uuid) {
        return get(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Get or create a UUID for the given text representation.
     *
     * @param s the text representation of the UUID
     * @return the UUID
     */
    public static ValueUuid get(String s) {
        long low = 0, high = 0;
        for (int i = 0, j = 0, length = s.length(); i < length; i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                low = (low << 4) | (c - '0');
            } else if (c >= 'a' && c <= 'f') {
                low = (low << 4) | (c - 'a' + 0xa);
            } else if (c == '-') {
                continue;
            } else if (c >= 'A' && c <= 'F') {
                low = (low << 4) | (c - 'A' + 0xa);
            } else if (c <= ' ') {
                continue;
            } else {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
            }
            if (j++ == 15) {
                high = low;
                low = 0;
            }
        }
        return (ValueUuid) Value.cache(new ValueUuid(high, low));
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append('\'');
        return addString(builder).append('\'');
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_UUID;
    }

    @Override
    public int getMemory() {
        return 32;
    }

    @Override
    public int getValueType() {
        return UUID;
    }

    @Override
    public String getString() {
        return addString(new StringBuilder(36)).toString();
    }

    private StringBuilder addString(StringBuilder builder) {
        StringUtils.appendHex(builder, high >> 32, 4).append('-');
        StringUtils.appendHex(builder, high >> 16, 2).append('-');
        StringUtils.appendHex(builder, high, 2).append('-');
        StringUtils.appendHex(builder, low >> 48, 2).append('-');
        return StringUtils.appendHex(builder, low, 6);
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode) {
        if (o == this) {
            return 0;
        }
        ValueUuid v = (ValueUuid) o;
        long v1 = high, v2 = v.high;
        if (v1 == v2) {
            v1 = low;
            v2 = v.low;
            if (mode.isUuidUnsigned()) {
                v1 += Long.MIN_VALUE;
                v2 += Long.MIN_VALUE;
            }
            return Long.compare(v1, v2);
        }
        if (mode.isUuidUnsigned()) {
            v1 += Long.MIN_VALUE;
            v2 += Long.MIN_VALUE;
        }
        return v1 > v2 ? 1 : -1;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueUuid)) {
            return false;
        }
        ValueUuid v = (ValueUuid) other;
        return high == v.high && low == v.low;
    }

    @Override
    public Object getObject() {
        return new UUID(high, low);
    }

    @Override
    public byte[] getBytes() {
        return Bits.uuidToBytes(high, low);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setBytes(parameterIndex, getBytes());
    }

    /**
     * Get the most significant 64 bits of this UUID.
     *
     * @return the high order bits
     */
    public long getHigh() {
        return high;
    }

    /**
     * Get the least significant 64 bits of this UUID.
     *
     * @return the low order bits
     */
    public long getLow() {
        return low;
    }

}
