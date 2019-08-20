/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.h2.api.ErrorCode;
import org.h2.engine.Mode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public abstract class Value {

    /**
     * The data type is unknown at this time.
     */
    public static final int UNKNOWN = -1;

    /**
     * The value type for NULL.
     */
    public static final int NULL = 0;

    /**
     * The value type for BOOLEAN values.
     */
    public static final int BOOLEAN = 1;

    /**
     * The value type for BYTE values.
     */
    public static final int BYTE = 2;

    /**
     * The value type for SHORT values.
     */
    public static final int SHORT = 3;

    /**
     * The value type for INT values.
     */
    public static final int INT = 4;

    /**
     * The value type for LONG values.
     */
    public static final int LONG = 5;

    /**
     * The value type for DECIMAL values.
     */
    public static final int DECIMAL = 6;

    /**
     * The value type for DOUBLE values.
     */
    public static final int DOUBLE = 7;

    /**
     * The value type for FLOAT values.
     */
    public static final int FLOAT = 8;

    /**
     * The value type for TIME values.
     */
    public static final int TIME = 9;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = 10;

    /**
     * The value type for TIMESTAMP values.
     */
    public static final int TIMESTAMP = 11;

    /**
     * The value type for BYTES values.
     */
    public static final int BYTES = 12;

    /**
     * The value type for STRING values.
     */
    public static final int STRING = 13;

    /**
     * The value type for case insensitive STRING values.
     */
    public static final int STRING_IGNORECASE = 14;

    /**
     * The value type for BLOB values.
     */
    public static final int BLOB = 15;

    /**
     * The value type for CLOB values.
     */
    public static final int CLOB = 16;

    /**
     * The value type for ARRAY values.
     */
    public static final int ARRAY = 17;

    /**
     * The value type for RESULT_SET values.
     */
    public static final int RESULT_SET = 18;
    /**
     * The value type for JAVA_OBJECT values.
     */
    public static final int JAVA_OBJECT = 19;

    /**
     * The value type for UUID values.
     */
    public static final int UUID = 20;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int STRING_FIXED = 21;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int GEOMETRY = 22;

    /**
     * 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.
     */

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    public static final int TIMESTAMP_TZ = 24;

    /**
     * The value type for ENUM values.
     */
    public static final int ENUM = 25;

    /**
     * The number of value types.
     */
    public static final int TYPE_COUNT = ENUM;

    private static SoftReference<Value[]> softCache =
            new SoftReference<>(null);
    private static final BigDecimal MAX_LONG_DECIMAL =
            BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG_DECIMAL =
            BigDecimal.valueOf(Long.MIN_VALUE);

    /**
     * Check the range of the parameters.
     *
     * @param zeroBasedOffset the offset (0 meaning no offset)
     * @param length the length of the target
     * @param dataSize the length of the source
     */
    static void rangeCheck(long zeroBasedOffset, long length, long dataSize) {
        if ((zeroBasedOffset | length) < 0 || length > dataSize - zeroBasedOffset) {
            if (zeroBasedOffset < 0 || zeroBasedOffset > dataSize) {
                throw DbException.getInvalidValueException("offset", zeroBasedOffset + 1);
            }
            throw DbException.getInvalidValueException("length", length);
        }
    }

    /**
     * Get the SQL expression for this value.
     *
     * @return the SQL expression
     */
    public abstract String getSQL();

    /**
     * Get the value type.
     *
     * @return the type
     */
    public abstract int getType();

    /**
     * Get the precision.
     *
     * @return the precision
     */
    public abstract long getPrecision();

    /**
     * Get the display size in characters.
     *
     * @return the display size
     */
    public abstract int getDisplaySize();

    /**
     * Get the memory used by this object.
     *
     * @return the memory used in bytes
     */
    public int getMemory() {
        return DataType.getDataType(getType()).memory;
    }

    /**
     * Get the value as a string.
     *
     * @return the string
     */
    public abstract String getString();

    /**
     * Get the value as an object.
     *
     * @return the object
     */
    public abstract Object getObject();

    /**
     * Set the value as a parameter in a prepared statement.
     *
     * @param prep the prepared statement
     * @param parameterIndex the parameter index
     */
    public abstract void set(PreparedStatement prep, int parameterIndex)
            throws SQLException;

    /**
     * Compare the value with another value of the same type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    protected abstract int compareSecure(Value v, CompareMode mode);

    @Override
    public abstract int hashCode();

    /**
     * Check if the two values have the same hash code. No data conversion is
     * made; this method returns false if the other object is not of the same
     * class. For some values, compareTo may return 0 even if equals return
     * false. Example: ValueDecimal 0.0 and 0.00.
     *
     * @param other the other value
     * @return true if they are equal
     */
    @Override
    public abstract boolean equals(Object other);

    /**
     * Get the order of this value type.
     *
     * @param type the value type
     * @return the order number
     */
    static int getOrder(int type) {
        switch (type) {
        case UNKNOWN:
            return 1_000;
        case NULL:
            return 2_000;
        case STRING:
            return 10_000;
        case CLOB:
            return 11_000;
        case STRING_FIXED:
            return 12_000;
        case STRING_IGNORECASE:
            return 13_000;
        case BOOLEAN:
            return 20_000;
        case BYTE:
            return 21_000;
        case SHORT:
            return 22_000;
        case INT:
            return 23_000;
        case LONG:
            return 24_000;
        case DECIMAL:
            return 25_000;
        case FLOAT:
            return 26_000;
        case DOUBLE:
            return 27_000;
        case TIME:
            return 30_000;
        case DATE:
            return 31_000;
        case TIMESTAMP:
            return 32_000;
        case TIMESTAMP_TZ:
            return 34_000;
        case BYTES:
            return 40_000;
        case BLOB:
            return 41_000;
        case JAVA_OBJECT:
            return 42_000;
        case UUID:
            return 43_000;
        case GEOMETRY:
            return 44_000;
        case ARRAY:
            return 50_000;
        case RESULT_SET:
            return 51_000;
        case ENUM:
            return 52_000;
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.getDataTypeOrder(type);
            }
            throw DbException.throwInternalError("type:"+type);
        }
    }

    /**
     * Get the higher value order type of two value types. If values need to be
     * converted to match the other operands value type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param t1 the first value type
     * @param t2 the second value type
     * @return the higher value type of the two
     */
    public static int getHigherOrder(int t1, int t2) {
        if (t1 == Value.UNKNOWN || t2 == Value.UNKNOWN) {
            if (t1 == t2) {
                throw DbException.get(
                        ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            } else if (t1 == Value.NULL) {
                throw DbException.get(
                        ErrorCode.UNKNOWN_DATA_TYPE_1, "NULL, ?");
            } else if (t2 == Value.NULL) {
                throw DbException.get(
                        ErrorCode.UNKNOWN_DATA_TYPE_1, "?, NULL");
            }
        }
        if (t1 == t2) {
            return t1;
        }
        int o1 = getOrder(t1);
        int o2 = getOrder(t2);
        return o1 > o2 ? t1 : t2;
    }

    /**
     * Check if a value is in the cache that is equal to this value. If yes,
     * this value should be used to save memory. If the value is not in the
     * cache yet, it is added.
     *
     * @param v the value to look for
     * @return the value in the cache or the value passed
     */
    static Value cache(Value v) {
        if (SysProperties.OBJECT_CACHE) {
            int hash = v.hashCode();
            if (softCache == null) {
                softCache = new SoftReference<>(null);
            }
            Value[] cache = softCache.get();
            if (cache == null) {
                cache = new Value[SysProperties.OBJECT_CACHE_SIZE];
                softCache = new SoftReference<>(cache);
            }
            int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
            Value cached = cache[index];
            if (cached != null) {
                if (cached.getType() == v.getType() && v.equals(cached)) {
                    // cacheHit++;
                    return cached;
                }
            }
            // cacheMiss++;
            // cache[cacheCleaner] = null;
            // cacheCleaner = (cacheCleaner + 1) &
            //     (Constants.OBJECT_CACHE_SIZE - 1);
            cache[index] = v;
        }
        return v;
    }

    /**
     * Clear the value cache. Used for testing.
     */
    public static void clearCache() {
        softCache = null;
    }

    public boolean getBoolean() {
        return ((ValueBoolean) convertTo(Value.BOOLEAN)).getBoolean();
    }

    public Date getDate() {
        return ((ValueDate) convertTo(Value.DATE)).getDate();
    }

    public Time getTime() {
        return ((ValueTime) convertTo(Value.TIME)).getTime();
    }

    public Timestamp getTimestamp() {
        return ((ValueTimestamp) convertTo(Value.TIMESTAMP)).getTimestamp();
    }

    public byte[] getBytes() {
        return ((ValueBytes) convertTo(Value.BYTES)).getBytes();
    }

    public byte[] getBytesNoCopy() {
        return ((ValueBytes) convertTo(Value.BYTES)).getBytesNoCopy();
    }

    public byte getByte() {
        return ((ValueByte) convertTo(Value.BYTE)).getByte();
    }

    public short getShort() {
        return ((ValueShort) convertTo(Value.SHORT)).getShort();
    }

    public BigDecimal getBigDecimal() {
        return ((ValueDecimal) convertTo(Value.DECIMAL)).getBigDecimal();
    }

    public double getDouble() {
        return ((ValueDouble) convertTo(Value.DOUBLE)).getDouble();
    }

    public float getFloat() {
        return ((ValueFloat) convertTo(Value.FLOAT)).getFloat();
    }

    public int getInt() {
        return ((ValueInt) convertTo(Value.INT)).getInt();
    }

    public long getLong() {
        return ((ValueLong) convertTo(Value.LONG)).getLong();
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(getBytesNoCopy());
    }

    /**
     * Get the input stream
     *
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the requested length
     * @return the new input stream
     */
    public InputStream getInputStream(long oneBasedOffset, long length) {
        byte[] bytes = getBytesNoCopy();
        long zeroBasedOffset = oneBasedOffset - 1;
        rangeCheck(zeroBasedOffset, length, bytes.length);
        return new ByteArrayInputStream(bytes, (int) zeroBasedOffset, (int) length);
    }

    public Reader getReader() {
        return new StringReader(getString());
    }

    /**
     * Get the reader
     *
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the requested length
     * @return the new reader
     */
    public Reader getReader(long oneBasedOffset, long length) {
        String string = getString();
        long zeroBasedOffset = oneBasedOffset - 1;
        rangeCheck(zeroBasedOffset, length, string.length());
        int offset = (int) zeroBasedOffset;
        return new StringReader(string.substring(offset, offset + (int) length));
    }

    /**
     * Add a value and return the result.
     *
     * @param v the value to add
     * @return the result
     */
    public Value add(@SuppressWarnings("unused") Value v) {
        throw throwUnsupportedExceptionForType("+");
    }

    public int getSignum() {
        throw throwUnsupportedExceptionForType("SIGNUM");
    }

    /**
     * Return -value if this value support arithmetic operations.
     *
     * @return the negative
     */
    public Value negate() {
        throw throwUnsupportedExceptionForType("NEG");
    }

    /**
     * Subtract a value and return the result.
     *
     * @param v the value to subtract
     * @return the result
     */
    public Value subtract(@SuppressWarnings("unused") Value v) {
        throw throwUnsupportedExceptionForType("-");
    }

    /**
     * Divide by a value and return the result.
     *
     * @param v the value to divide by
     * @return the result
     */
    public Value divide(@SuppressWarnings("unused") Value v) {
        throw throwUnsupportedExceptionForType("/");
    }

    /**
     * Multiply with a value and return the result.
     *
     * @param v the value to multiply with
     * @return the result
     */
    public Value multiply(@SuppressWarnings("unused") Value v) {
        throw throwUnsupportedExceptionForType("*");
    }

    /**
     * Take the modulus with a value and return the result.
     *
     * @param v the value to take the modulus with
     * @return the result
     */
    public Value modulus(@SuppressWarnings("unused") Value v) {
        throw throwUnsupportedExceptionForType("%");
    }

    /**
     * Compare a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @return the converted value
     */
    public Value convertTo(int targetType) {
        // Use -1 to indicate "default behaviour" where value conversion should not
        // depend on any datatype precision.
        return convertTo(targetType, -1, null);
    }

    /**
     * Convert value to ENUM value
     * @param enumerators allowed values for the ENUM to which the value is converted
     * @return value represented as ENUM
     */
    public Value convertToEnum(String[] enumerators) {
        // Use -1 to indicate "default behaviour" where value conversion should not
        // depend on any datatype precision.
        return convertTo(ENUM, -1, null, null, enumerators);
    }

    /**
     * Compare a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @param precision the precision of the column to convert this value to.
     *        The special constant <code>-1</code> is used to indicate that
     *        the precision plays no role when converting the value
     * @param mode the mode
     * @return the converted value
     */
    public final Value convertTo(int targetType, int precision, Mode mode) {
        return convertTo(targetType, precision, mode, null, null);
    }

    /**
     * Compare a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @param precision the precision of the column to convert this value to.
     *        The special constant <code>-1</code> is used to indicate that
     *        the precision plays no role when converting the value
     * @param mode the conversion mode
     * @param column the column (if any), used for to improve the error message if conversion fails
     * @param enumerators the ENUM datatype enumerators (if any),
     *        for dealing with ENUM conversions
     * @return the converted value
     */
    public Value convertTo(int targetType, int precision, Mode mode, Object column, String[] enumerators) {
        // converting NULL is done in ValueNull
        // converting BLOB to CLOB and vice versa is done in ValueLob
        if (getType() == targetType) {
            return this;
        }
        try {
            // decimal conversion
            switch (targetType) {
            case BOOLEAN: {
                switch (getType()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case DECIMAL:
                case DOUBLE:
                case FLOAT:
                    return ValueBoolean.get(getSignum() != 0);
                case TIME:
                case DATE:
                case TIMESTAMP:
                case TIMESTAMP_TZ:
                case BYTES:
                case JAVA_OBJECT:
                case UUID:
                case ENUM:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case BYTE: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueByte.get(getBoolean() ? (byte) 1 : (byte) 0);
                case SHORT:
                case ENUM:
                case INT:
                    return ValueByte.get(convertToByte(getInt(), column));
                case LONG:
                    return ValueByte.get(convertToByte(getLong(), column));
                case DECIMAL:
                    return ValueByte.get(convertToByte(convertToLong(getBigDecimal(), column), column));
                case DOUBLE:
                    return ValueByte.get(convertToByte(convertToLong(getDouble(), column), column));
                case FLOAT:
                    return ValueByte.get(convertToByte(convertToLong(getFloat(), column), column));
                case BYTES:
                    return ValueByte.get((byte) Integer.parseInt(getString(), 16));
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case SHORT: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueShort.get(getBoolean() ? (short) 1 : (short) 0);
                case BYTE:
                    return ValueShort.get(getByte());
                case ENUM:
                case INT:
                    return ValueShort.get(convertToShort(getInt(), column));
                case LONG:
                    return ValueShort.get(convertToShort(getLong(), column));
                case DECIMAL:
                    return ValueShort.get(convertToShort(convertToLong(getBigDecimal(), column), column));
                case DOUBLE:
                    return ValueShort.get(convertToShort(convertToLong(getDouble(), column), column));
                case FLOAT:
                    return ValueShort.get(convertToShort(convertToLong(getFloat(), column), column));
                case BYTES:
                    return ValueShort.get((short) Integer.parseInt(getString(), 16));
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case INT: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueInt.get(getBoolean() ? 1 : 0);
                case BYTE:
                case ENUM:
                case SHORT:
                    return ValueInt.get(getInt());
                case LONG:
                    return ValueInt.get(convertToInt(getLong(), column));
                case DECIMAL:
                    return ValueInt.get(convertToInt(convertToLong(getBigDecimal(), column), column));
                case DOUBLE:
                    return ValueInt.get(convertToInt(convertToLong(getDouble(), column), column));
                case FLOAT:
                    return ValueInt.get(convertToInt(convertToLong(getFloat(), column), column));
                case BYTES:
                    return ValueInt.get((int) Long.parseLong(getString(), 16));
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case LONG: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueLong.get(getBoolean() ? 1 : 0);
                case BYTE:
                case SHORT:
                case ENUM:
                case INT:
                    return ValueLong.get(getInt());
                case DECIMAL:
                    return ValueLong.get(convertToLong(getBigDecimal(), column));
                case DOUBLE:
                    return ValueLong.get(convertToLong(getDouble(), column));
                case FLOAT:
                    return ValueLong.get(convertToLong(getFloat(), column));
                case BYTES: {
                    // parseLong doesn't work for ffffffffffffffff
                    byte[] d = getBytes();
                    if (d.length == 8) {
                        return ValueLong.get(Bits.readLong(d, 0));
                    }
                    return ValueLong.get(Long.parseLong(getString(), 16));
                }
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case DECIMAL: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueDecimal.get(BigDecimal.valueOf(getBoolean() ? 1 : 0));
                case BYTE:
                case SHORT:
                case ENUM:
                case INT:
                    return ValueDecimal.get(BigDecimal.valueOf(getInt()));
                case LONG:
                    return ValueDecimal.get(BigDecimal.valueOf(getLong()));
                case DOUBLE: {
                    double d = getDouble();
                    if (Double.isInfinite(d) || Double.isNaN(d)) {
                        throw DbException.get(
                                ErrorCode.DATA_CONVERSION_ERROR_1, "" + d);
                    }
                    return ValueDecimal.get(BigDecimal.valueOf(d));
                }
                case FLOAT: {
                    float f = getFloat();
                    if (Float.isInfinite(f) || Float.isNaN(f)) {
                        throw DbException.get(
                                ErrorCode.DATA_CONVERSION_ERROR_1, "" + f);
                    }
                    // better rounding behavior than BigDecimal.valueOf(f)
                    return ValueDecimal.get(new BigDecimal(Float.toString(f)));
                }
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case DOUBLE: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueDouble.get(getBoolean() ? 1 : 0);
                case BYTE:
                case SHORT:
                case INT:
                    return ValueDouble.get(getInt());
                case LONG:
                    return ValueDouble.get(getLong());
                case DECIMAL:
                    return ValueDouble.get(getBigDecimal().doubleValue());
                case FLOAT:
                    return ValueDouble.get(getFloat());
                case ENUM:
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case FLOAT: {
                switch (getType()) {
                case BOOLEAN:
                    return ValueFloat.get(getBoolean() ? 1 : 0);
                case BYTE:
                case SHORT:
                case INT:
                    return ValueFloat.get(getInt());
                case LONG:
                    return ValueFloat.get(getLong());
                case DECIMAL:
                    return ValueFloat.get(getBigDecimal().floatValue());
                case DOUBLE:
                    return ValueFloat.get((float) getDouble());
                case ENUM:
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case DATE: {
                switch (getType()) {
                case TIME:
                    // because the time has set the date to 1970-01-01,
                    // this will be the result
                    return ValueDate.fromDateValue(DateTimeUtils.EPOCH_DATE_VALUE);
                case TIMESTAMP:
                    return ValueDate.fromDateValue(
                            ((ValueTimestamp) this).getDateValue());
                case TIMESTAMP_TZ: {
                    ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
                    long dateValue = ts.getDateValue(), timeNanos = ts.getTimeNanos();
                    long millis = DateTimeUtils.getMillis(dateValue, timeNanos, ts.getTimeZoneOffsetMins());
                    return ValueDate.fromMillis(millis);
                }
                case ENUM:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case TIME: {
                switch (getType()) {
                case DATE:
                    // need to normalize the year, month and day because a date
                    // has the time set to 0, the result will be 0
                    return ValueTime.fromNanos(0);
                case TIMESTAMP:
                    return ValueTime.fromNanos(
                            ((ValueTimestamp) this).getTimeNanos());
                case TIMESTAMP_TZ: {
                    ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
                    long dateValue = ts.getDateValue(), timeNanos = ts.getTimeNanos();
                    long millis = DateTimeUtils.getMillis(dateValue, timeNanos, ts.getTimeZoneOffsetMins());
                    return ValueTime.fromNanos(DateTimeUtils.nanosFromDate(millis) + timeNanos % 1_000_000);
                }
                case ENUM:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case TIMESTAMP: {
                switch (getType()) {
                case TIME:
                    return DateTimeUtils.normalizeTimestamp(
                            0, ((ValueTime) this).getNanos());
                case DATE:
                    return ValueTimestamp.fromDateValueAndNanos(
                            ((ValueDate) this).getDateValue(), 0);
                case TIMESTAMP_TZ: {
                    ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
                    long dateValue = ts.getDateValue(), timeNanos = ts.getTimeNanos();
                    long millis = DateTimeUtils.getMillis(dateValue, timeNanos, ts.getTimeZoneOffsetMins());
                    return ValueTimestamp.fromMillisNanos(millis, (int) (timeNanos % 1_000_000));
                }
                case ENUM:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case TIMESTAMP_TZ: {
                switch (getType()) {
                case TIME: {
                    ValueTimestamp ts = DateTimeUtils.normalizeTimestamp(0, ((ValueTime) this).getNanos());
                    return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(
                            ts.getDateValue(), ts.getTimeNanos());
                }
                case DATE:
                    return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(
                            ((ValueDate) this).getDateValue(), 0);
                case TIMESTAMP: {
                    ValueTimestamp ts = (ValueTimestamp) this;
                    return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(
                            ts.getDateValue(), ts.getTimeNanos());
                }
                case ENUM:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case BYTES: {
                switch (getType()) {
                case JAVA_OBJECT:
                case BLOB:
                    return ValueBytes.getNoCopy(getBytesNoCopy());
                case UUID:
                case GEOMETRY:
                    return ValueBytes.getNoCopy(getBytes());
                case BYTE:
                    return ValueBytes.getNoCopy(new byte[]{getByte()});
                case SHORT: {
                    int x = getShort();
                    return ValueBytes.getNoCopy(new byte[]{
                            (byte) (x >> 8),
                            (byte) x
                    });
                }
                case INT: {
                    byte[] b = new byte[4];
                    Bits.writeInt(b, 0, getInt());
                    return ValueBytes.getNoCopy(b);
                }
                case LONG: {
                    byte[] b = new byte[8];
                    Bits.writeLong(b, 0, getLong());
                    return ValueBytes.getNoCopy(b);
                }
                case ENUM:
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case JAVA_OBJECT: {
                switch (getType()) {
                case BYTES:
                case BLOB:
                    return ValueJavaObject.getNoCopy(
                            null, getBytesNoCopy(), getDataHandler());
                case ENUM:
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case ENUM: {
                switch (getType()) {
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case DECIMAL:
                        return ValueEnum.get(enumerators, getInt());
                    case STRING:
                    case STRING_IGNORECASE:
                    case STRING_FIXED:
                        return ValueEnum.get(enumerators, getString());
                    default:
                        throw DbException.get(
                                ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
            }
            case BLOB: {
                switch (getType()) {
                case BYTES:
                    return ValueLobDb.createSmallLob(
                            Value.BLOB, getBytesNoCopy());
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case UUID: {
                switch (getType()) {
                case BYTES:
                    return ValueUuid.get(getBytesNoCopy());
                case JAVA_OBJECT:
                    Object object = JdbcUtils.deserialize(getBytesNoCopy(),
                            getDataHandler());
                    if (object instanceof java.util.UUID) {
                        return ValueUuid.get((java.util.UUID) object);
                    }
                    throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            case GEOMETRY: {
                switch (getType()) {
                case BYTES:
                    return ValueGeometry.get(getBytesNoCopy());
                case JAVA_OBJECT:
                    Object object = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
                    if (DataType.isGeometry(object)) {
                        return ValueGeometry.getFromGeometry(object);
                    }
                    //$FALL-THROUGH$
                case TIMESTAMP_TZ:
                    throw DbException.get(
                            ErrorCode.DATA_CONVERSION_ERROR_1, getString());
                }
                break;
            }
            }
            // conversion by parsing the string value
            String s = getString();
            switch (targetType) {
            case NULL:
                return ValueNull.INSTANCE;
            case BOOLEAN: {
                if (s.equalsIgnoreCase("true") ||
                        s.equalsIgnoreCase("t") ||
                        s.equalsIgnoreCase("yes") ||
                        s.equalsIgnoreCase("y")) {
                    return ValueBoolean.TRUE;
                } else if (s.equalsIgnoreCase("false") ||
                        s.equalsIgnoreCase("f") ||
                        s.equalsIgnoreCase("no") ||
                        s.equalsIgnoreCase("n")) {
                    return ValueBoolean.FALSE;
                } else {
                    // convert to a number, and if it is not 0 then it is true
                    return ValueBoolean.get(new BigDecimal(s).signum() != 0);
                }
            }
            case BYTE:
                return ValueByte.get(Byte.parseByte(s.trim()));
            case SHORT:
                return ValueShort.get(Short.parseShort(s.trim()));
            case INT:
                return ValueInt.get(Integer.parseInt(s.trim()));
            case LONG:
                return ValueLong.get(Long.parseLong(s.trim()));
            case DECIMAL:
                return ValueDecimal.get(new BigDecimal(s.trim()));
            case TIME:
                return ValueTime.parse(s.trim());
            case DATE:
                return ValueDate.parse(s.trim());
            case TIMESTAMP:
                return ValueTimestamp.parse(s.trim(), mode);
            case TIMESTAMP_TZ:
                return ValueTimestampTimeZone.parse(s.trim());
            case BYTES:
                return ValueBytes.getNoCopy(
                        StringUtils.convertHexToBytes(s.trim()));
            case JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(null,
                        StringUtils.convertHexToBytes(s.trim()), getDataHandler());
            case STRING:
                return ValueString.get(s);
            case STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(s);
            case STRING_FIXED:
                return ValueStringFixed.get(s, precision, mode);
            case DOUBLE:
                return ValueDouble.get(Double.parseDouble(s.trim()));
            case FLOAT:
                return ValueFloat.get(Float.parseFloat(s.trim()));
            case CLOB:
                return ValueLobDb.createSmallLob(
                        CLOB, s.getBytes(StandardCharsets.UTF_8));
            case BLOB:
                return ValueLobDb.createSmallLob(
                        BLOB, StringUtils.convertHexToBytes(s.trim()));
            case ARRAY:
                return ValueArray.get(new Value[]{ValueString.get(s)});
            case RESULT_SET: {
                SimpleResultSet rs = new SimpleResultSet();
                rs.setAutoClose(false);
                rs.addColumn("X", Types.VARCHAR, s.length(), 0);
                rs.addRow(s);
                return ValueResultSet.get(rs);
            }
            case UUID:
                return ValueUuid.get(s);
            case GEOMETRY:
                return ValueGeometry.get(s);
            default:
                if (JdbcUtils.customDataTypesHandler != null) {
                    return JdbcUtils.customDataTypesHandler.convert(this, targetType);
                }
                throw DbException.throwInternalError("type=" + targetType);
            }
        } catch (NumberFormatException e) {
            throw DbException.get(
                    ErrorCode.DATA_CONVERSION_ERROR_1, e, getString());
        }
    }

    /**
     * Compare this value against another value given that the values are of the
     * same data type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public final int compareTypeSafe(Value v, CompareMode mode) {
        if (this == v) {
            return 0;
        } else if (this == ValueNull.INSTANCE) {
            return -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        return compareSecure(v, mode);
    }

    /**
     * Compare this value against another value using the specified compare
     * mode.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public final int compareTo(Value v, CompareMode mode) {
        if (this == v) {
            return 0;
        }
        if (this == ValueNull.INSTANCE) {
            return v == ValueNull.INSTANCE ? 0 : -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        if (getType() == v.getType()) {
            return compareSecure(v, mode);
        }
        int t2 = Value.getHigherOrder(getType(), v.getType());
        return convertTo(t2).compareSecure(v.convertTo(t2), mode);
    }

    public int getScale() {
        return 0;
    }

    /**
     * Convert the scale.
     *
     * @param onlyToSmallerScale if the scale should not reduced
     * @param targetScale the requested scale
     * @return the value
     */
    @SuppressWarnings("unused")
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        return this;
    }

    /**
     * Convert the precision to the requested value. The precision of the
     * returned value may be somewhat larger than requested, because values with
     * a fixed precision are not truncated.
     *
     * @param precision the new precision
     * @param force true if losing numeric precision is allowed
     * @return the new value
     */
    @SuppressWarnings("unused")
    public Value convertPrecision(long precision, boolean force) {
        return this;
    }

    private static byte convertToByte(long x, Object column) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Long.toString(x), getColumnName(column));
        }
        return (byte) x;
    }

    private static short convertToShort(long x, Object column) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Long.toString(x), getColumnName(column));
        }
        return (short) x;
    }

    private static int convertToInt(long x, Object column) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Long.toString(x), getColumnName(column));
        }
        return (int) x;
    }

    private static long convertToLong(double x, Object column) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            // TODO document that +Infinity, -Infinity throw an exception and
            // NaN returns 0
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, Double.toString(x), getColumnName(column));
        }
        return Math.round(x);
    }

    private static long convertToLong(BigDecimal x, Object column) {
        if (x.compareTo(MAX_LONG_DECIMAL) > 0 ||
                x.compareTo(Value.MIN_LONG_DECIMAL) < 0) {
            throw DbException.get(
                    ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_2, x.toString(), getColumnName(column));
        }
        return x.setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
    }

    private static String getColumnName(Object column) {
        return column == null ? "" : column.toString();
    }

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param handler the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    @SuppressWarnings("unused")
    public Value copy(DataHandler handler, int tableId) {
        return this;
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isLinkedToTable() {
        return false;
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    public void remove() {
        // nothing to do
    }

    /**
     * Check if the precision is smaller or equal than the given precision.
     *
     * @param precision the maximum precision
     * @return true if the precision of this value is smaller or equal to the
     *         given precision
     */
    public boolean checkPrecision(long precision) {
        return getPrecision() <= precision;
    }

    /**
     * Get a medium size SQL expression for debugging or tracing. If the
     * precision is too large, only a subset of the value is returned.
     *
     * @return the SQL expression
     */
    public String getTraceSQL() {
        return getSQL();
    }

    @Override
    public String toString() {
        return getTraceSQL();
    }

    /**
     * Throw the exception that the feature is not support for the given data
     * type.
     *
     * @param op the operation
     * @return never returns normally
     * @throws DbException the exception
     */
    protected DbException throwUnsupportedExceptionForType(String op) {
        throw DbException.getUnsupportedException(
                DataType.getDataType(getType()).name + " " + op);
    }

    /**
     * Get the table (only for LOB object).
     *
     * @return the table id
     */
    public int getTableId() {
        return 0;
    }

    /**
     * Get the byte array.
     *
     * @return the byte array
     */
    public byte[] getSmall() {
        return null;
    }

    /**
     * Copy this value to a temporary file if necessary.
     *
     * @return the new value
     */
    public Value copyToTemp() {
        return this;
    }

    /**
     * Create an independent copy of this value if needed, that will be bound to
     * a result. If the original row is removed, this copy is still readable.
     *
     * @return the value (this for small objects)
     */
    public Value copyToResult() {
        return this;
    }

    public ResultSet getResultSet() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.setAutoClose(false);
        rs.addColumn("X", DataType.convertTypeToSQLType(getType()),
                MathUtils.convertLongToInt(getPrecision()), getScale());
        rs.addRow(getObject());
        return rs;
    }

    /**
     * Return the data handler for the values that support it
     * (actually only Java objects).
     * @return the data handler
     */
    protected DataHandler getDataHandler() {
        return null;
    }

    /**
     * A "binary large object".
     */
    public interface ValueClob {
        // this is a marker interface
    }

    /**
     * A "character large object".
     */
    public interface ValueBlob {
        // this is a marker interface
    }

}
