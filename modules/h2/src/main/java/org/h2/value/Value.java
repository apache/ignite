/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Mode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.store.DataHandler;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.IntervalUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public abstract class Value extends VersionedValue {

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
     * The value type for {@code INTERVAL YEAR} values.
     */
    public static final int INTERVAL_YEAR = 26;

    /**
     * The value type for {@code INTERVAL MONTH} values.
     */
    public static final int INTERVAL_MONTH = 27;

    /**
     * The value type for {@code INTERVAL DAY} values.
     */
    public static final int INTERVAL_DAY = 28;

    /**
     * The value type for {@code INTERVAL HOUR} values.
     */
    public static final int INTERVAL_HOUR = 29;

    /**
     * The value type for {@code INTERVAL MINUTE} values.
     */
    public static final int INTERVAL_MINUTE = 30;

    /**
     * The value type for {@code INTERVAL SECOND} values.
     */
    public static final int INTERVAL_SECOND = 31;

    /**
     * The value type for {@code INTERVAL YEAR TO MONTH} values.
     */
    public static final int INTERVAL_YEAR_TO_MONTH = 32;

    /**
     * The value type for {@code INTERVAL DAY TO HOUR} values.
     */
    public static final int INTERVAL_DAY_TO_HOUR = 33;

    /**
     * The value type for {@code INTERVAL DAY TO MINUTE} values.
     */
    public static final int INTERVAL_DAY_TO_MINUTE = 34;

    /**
     * The value type for {@code INTERVAL DAY TO SECOND} values.
     */
    public static final int INTERVAL_DAY_TO_SECOND = 35;

    /**
     * The value type for {@code INTERVAL HOUR TO MINUTE} values.
     */
    public static final int INTERVAL_HOUR_TO_MINUTE = 36;

    /**
     * The value type for {@code INTERVAL HOUR TO SECOND} values.
     */
    public static final int INTERVAL_HOUR_TO_SECOND = 37;

    /**
     * The value type for {@code INTERVAL MINUTE TO SECOND} values.
     */
    public static final int INTERVAL_MINUTE_TO_SECOND = 38;

    /**
     * The value type for ROW values.
     */
    public static final int ROW = 39;

    /**
     * The number of value types.
     */
    public static final int TYPE_COUNT = ROW + 1;

    private static SoftReference<Value[]> softCache;

    private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    /**
     * The smallest Long value, as a BigDecimal.
     */
    public static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

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
    public String getSQL() {
        return getSQL(new StringBuilder()).toString();
    }

    /**
     * Appends the SQL expression for this value to the specified builder.
     *
     * @param builder
     *            string builder
     * @return the specified string builder
     */
    public abstract StringBuilder getSQL(StringBuilder builder);

    /**
     * Returns the data type.
     *
     * @return the data type
     */
    public abstract TypeInfo getType();

    /**
     * Get the value type.
     *
     * @return the value type
     */
    public abstract int getValueType();

    /**
     * Get the memory used by this object.
     *
     * @return the memory used in bytes
     */
    public int getMemory() {
        /*
         * Java 11 with -XX:-UseCompressedOops for all values up to ValueLong
         * and ValueDouble.
         */
        return 24;
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
        case INTERVAL_YEAR:
            return 28_000;
        case INTERVAL_MONTH:
            return 28_100;
        case INTERVAL_YEAR_TO_MONTH:
            return 28_200;
        case INTERVAL_DAY:
            return 29_000;
        case INTERVAL_HOUR:
            return 29_100;
        case INTERVAL_DAY_TO_HOUR:
            return 29_200;
        case INTERVAL_MINUTE:
            return 29_300;
        case INTERVAL_HOUR_TO_MINUTE:
            return 29_400;
        case INTERVAL_DAY_TO_MINUTE:
            return 29_500;
        case INTERVAL_SECOND:
            return 29_600;
        case INTERVAL_MINUTE_TO_SECOND:
            return 29_700;
        case INTERVAL_HOUR_TO_SECOND:
            return 29_800;
        case INTERVAL_DAY_TO_SECOND:
            return 29_900;
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
        case ENUM:
            return 45_000;
        case ARRAY:
            return 50_000;
        case ROW:
            return 51_000;
        case RESULT_SET:
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
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            } else if (t1 == Value.NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "NULL, ?");
            } else if (t2 == Value.NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, NULL");
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
     * Get the higher data type of two data types. If values need to be
     * converted to match the other operands data type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param type1 the first data type
     * @param type2 the second data type
     * @return the higher data type of the two
     */
    public static TypeInfo getHigherType(TypeInfo type1, TypeInfo type2) {
        int t1 = type1.getValueType(), t2 = type2.getValueType();
        int dataType = getHigherOrder(t1, t2);
        long precision = Math.max(type1.getPrecision(), type2.getPrecision());
        int scale = Math.max(type1.getScale(), type2.getScale());
        ExtTypeInfo ext1 = type1.getExtTypeInfo();
        ExtTypeInfo ext = dataType == t1 && ext1 != null ? ext1 : dataType == t2 ? type2.getExtTypeInfo() : null;
        return TypeInfo.getTypeInfo(dataType, precision, scale, ext);
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
            Value[] cache;
            if (softCache == null || (cache = softCache.get()) == null) {
                cache = new Value[SysProperties.OBJECT_CACHE_SIZE];
                softCache = new SoftReference<>(cache);
            }
            int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
            Value cached = cache[index];
            if (cached != null) {
                if (cached.getValueType() == v.getValueType() && v.equals(cached)) {
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
        throw getUnsupportedExceptionForOperation("+");
    }

    public int getSignum() {
        throw getUnsupportedExceptionForOperation("SIGNUM");
    }

    /**
     * Return -value if this value support arithmetic operations.
     *
     * @return the negative
     */
    public Value negate() {
        throw getUnsupportedExceptionForOperation("NEG");
    }

    /**
     * Subtract a value and return the result.
     *
     * @param v the value to subtract
     * @return the result
     */
    public Value subtract(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("-");
    }

    /**
     * Divide by a value and return the result.
     *
     * @param v the value to divide by
     * @return the result
     */
    public Value divide(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("/");
    }

    /**
     * Multiply with a value and return the result.
     *
     * @param v the value to multiply with
     * @return the result
     */
    public Value multiply(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("*");
    }

    /**
     * Take the modulus with a value and return the result.
     *
     * @param v the value to take the modulus with
     * @return the result
     */
    public Value modulus(@SuppressWarnings("unused") Value v) {
        throw getUnsupportedExceptionForOperation("%");
    }

    /**
     * Compare a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @return the converted value
     */
    public final Value convertTo(int targetType) {
        return convertTo(targetType, null, null, null);
    }

    /**
     * Convert value to ENUM value
     * @param enumerators the extended type information for the ENUM data type
     * @return value represented as ENUM
     */
    private Value convertToEnum(ExtTypeInfo enumerators) {
        return convertTo(ENUM, null, null, enumerators);
    }

    /**
     * Convert a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @param mode the mode
     * @return the converted value
     */
    public final Value convertTo(int targetType, Mode mode) {
        return convertTo(targetType, mode, null, null);
    }

    /**
     * Convert a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @param mode the conversion mode
     * @param column the column (if any), used for to improve the error message if conversion fails
     * @return the converted value
     */
    public final Value convertTo(TypeInfo targetType, Mode mode, Object column) {
        return convertTo(targetType.getValueType(), mode, column, targetType.getExtTypeInfo());
    }

    /**
     * Convert a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @param mode the conversion mode
     * @param column the column (if any), used for to improve the error message if conversion fails
     * @param extTypeInfo the extended data type information, or null
     * @return the converted value
     */
    protected Value convertTo(int targetType, Mode mode, Object column, ExtTypeInfo extTypeInfo) {
        // converting NULL is done in ValueNull
        // converting BLOB to CLOB and vice versa is done in ValueLob
        if (getValueType() == targetType) {
            return this;
        }
        try {
            switch (targetType) {
            case NULL:
                return ValueNull.INSTANCE;
            case BOOLEAN:
                return convertToBoolean();
            case BYTE:
                return convertToByte(column);
            case SHORT:
                return convertToShort(column);
            case INT:
                return convertToInt(column);
            case LONG:
                return convertToLong(column);
            case DECIMAL:
                return convertToDecimal();
            case DOUBLE:
                return convertToDouble();
            case FLOAT:
                return convertToFloat();
            case DATE:
                return convertToDate();
            case TIME:
                return convertToTime();
            case TIMESTAMP:
                return convertToTimestamp(mode);
            case TIMESTAMP_TZ:
                return convertToTimestampTimeZone();
            case BYTES:
                return convertToBytes(mode);
            case STRING:
                return convertToString(mode);
            case STRING_IGNORECASE:
                return convertToStringIgnoreCase(mode);
            case STRING_FIXED:
                return convertToStringFixed(mode);
            case JAVA_OBJECT:
                return convertToJavaObject();
            case ENUM:
                return convertToEnumInternal((ExtTypeInfoEnum) extTypeInfo);
            case BLOB:
                return convertToBlob();
            case CLOB:
                return convertToClob();
            case UUID:
                return convertToUuid();
            case GEOMETRY:
                return convertToGeometry((ExtTypeInfoGeometry) extTypeInfo);
            case Value.INTERVAL_YEAR:
            case Value.INTERVAL_MONTH:
            case Value.INTERVAL_YEAR_TO_MONTH:
                return convertToIntervalYearMonth(targetType);
            case Value.INTERVAL_DAY:
            case Value.INTERVAL_HOUR:
            case Value.INTERVAL_MINUTE:
            case Value.INTERVAL_SECOND:
            case Value.INTERVAL_DAY_TO_HOUR:
            case Value.INTERVAL_DAY_TO_MINUTE:
            case Value.INTERVAL_DAY_TO_SECOND:
            case Value.INTERVAL_HOUR_TO_MINUTE:
            case Value.INTERVAL_HOUR_TO_SECOND:
            case Value.INTERVAL_MINUTE_TO_SECOND:
                return convertToIntervalDayTime(targetType);
            case ARRAY:
                return convertToArray();
            case ROW:
                return convertToRow();
            case RESULT_SET:
                return convertToResultSet();
            default:
                if (JdbcUtils.customDataTypesHandler != null) {
                    return JdbcUtils.customDataTypesHandler.convert(this, targetType);
                }
                throw getDataConversionError(targetType);
            }
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, getString());
        }
    }

    private ValueBoolean convertToBoolean() {
        switch (getValueType()) {
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
            throw getDataConversionError(BOOLEAN);
        }
        String s = getString();
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t") || s.equalsIgnoreCase("yes")
                || s.equalsIgnoreCase("y")) {
            return ValueBoolean.TRUE;
        } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f") || s.equalsIgnoreCase("no")
                || s.equalsIgnoreCase("n")) {
            return ValueBoolean.FALSE;
        } else {
            // convert to a number, and if it is not 0 then it is true
            return ValueBoolean.get(new BigDecimal(s).signum() != 0);
        }
    }

    private ValueByte convertToByte(Object column) {
        switch (getValueType()) {
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
            throw getDataConversionError(BYTE);
        }
        return ValueByte.get(Byte.parseByte(getString().trim()));
    }

    private ValueShort convertToShort(Object column) {
        switch (getValueType()) {
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
            throw getDataConversionError(SHORT);
        }
        return ValueShort.get(Short.parseShort(getString().trim()));
    }

    private ValueInt convertToInt(Object column) {
        switch (getValueType()) {
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
            throw getDataConversionError(INT);
        }
        return ValueInt.get(Integer.parseInt(getString().trim()));
    }

    private ValueLong convertToLong(Object column) {
        switch (getValueType()) {
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
            throw getDataConversionError(LONG);
        }
        return ValueLong.get(Long.parseLong(getString().trim()));
    }

    private ValueDecimal convertToDecimal() {
        switch (getValueType()) {
        case BOOLEAN:
            return (ValueDecimal) (getBoolean() ? ValueDecimal.ONE : ValueDecimal.ZERO);
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
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, Double.toString(d));
            }
            return ValueDecimal.get(BigDecimal.valueOf(d));
        }
        case FLOAT: {
            float f = getFloat();
            if (Float.isInfinite(f) || Float.isNaN(f)) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, Float.toString(f));
            }
            // better rounding behavior than BigDecimal.valueOf(f)
            return ValueDecimal.get(new BigDecimal(Float.toString(f)));
        }
        case TIMESTAMP_TZ:
            throw getDataConversionError(DECIMAL);
        }
        return ValueDecimal.get(new BigDecimal(getString().trim()));
    }

    private ValueDouble convertToDouble() {
        switch (getValueType()) {
        case BOOLEAN:
            return getBoolean() ? ValueDouble.ONE : ValueDouble.ZERO;
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
            throw getDataConversionError(DOUBLE);
        }
        return ValueDouble.get(Double.parseDouble(getString().trim()));
    }

    private ValueFloat convertToFloat() {
        switch (getValueType()) {
        case BOOLEAN:
            return getBoolean() ? ValueFloat.ONE : ValueFloat.ZERO;
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
            throw getDataConversionError(FLOAT);
        }
        return ValueFloat.get(Float.parseFloat(getString().trim()));
    }

    private ValueDate convertToDate() {
        switch (getValueType()) {
        case TIME:
            // because the time has set the date to 1970-01-01,
            // this will be the result
            return ValueDate.fromDateValue(DateTimeUtils.EPOCH_DATE_VALUE);
        case TIMESTAMP:
            return ValueDate.fromDateValue(((ValueTimestamp) this).getDateValue());
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            long dateValue = ts.getDateValue(), timeNanos = ts.getTimeNanos();
            long millis = DateTimeUtils.getMillis(dateValue, timeNanos, ts.getTimeZoneOffsetMins());
            return ValueDate.fromMillis(millis);
        }
        case ENUM:
            throw getDataConversionError(DATE);
        }
        return ValueDate.parse(getString().trim());
    }

    private ValueTime convertToTime() {
        switch (getValueType()) {
        case DATE:
            // need to normalize the year, month and day because a date
            // has the time set to 0, the result will be 0
            return ValueTime.fromNanos(0);
        case TIMESTAMP:
            return ValueTime.fromNanos(((ValueTimestamp) this).getTimeNanos());
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            long dateValue = ts.getDateValue(), timeNanos = ts.getTimeNanos();
            long millis = DateTimeUtils.getMillis(dateValue, timeNanos, ts.getTimeZoneOffsetMins());
            return ValueTime.fromNanos(
                    DateTimeUtils.nanosFromLocalMillis(millis + DateTimeUtils.getTimeZoneOffset(millis))
                            + timeNanos % 1_000_000);
        }
        case ENUM:
            throw getDataConversionError(TIME);
        }
        return ValueTime.parse(getString().trim());
    }

    private ValueTimestamp convertToTimestamp(Mode mode) {
        switch (getValueType()) {
        case TIME:
            return DateTimeUtils.normalizeTimestamp(0, ((ValueTime) this).getNanos());
        case DATE:
            return ValueTimestamp.fromDateValueAndNanos(((ValueDate) this).getDateValue(), 0);
        case TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) this;
            long dateValue = ts.getDateValue(), timeNanos = ts.getTimeNanos();
            long millis = DateTimeUtils.getMillis(dateValue, timeNanos, ts.getTimeZoneOffsetMins());
            return ValueTimestamp.fromMillisNanos(millis, (int) (timeNanos % 1_000_000));
        }
        case ENUM:
            throw getDataConversionError(TIMESTAMP);
        }
        return ValueTimestamp.parse(getString().trim(), mode);
    }

    private ValueTimestampTimeZone convertToTimestampTimeZone() {
        switch (getValueType()) {
        case TIME: {
            ValueTimestamp ts = DateTimeUtils.normalizeTimestamp(0, ((ValueTime) this).getNanos());
            return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(ts.getDateValue(), ts.getTimeNanos());
        }
        case DATE:
            return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(((ValueDate) this).getDateValue(), 0);
        case TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) this;
            return DateTimeUtils.timestampTimeZoneFromLocalDateValueAndNanos(ts.getDateValue(), ts.getTimeNanos());
        }
        case ENUM:
            throw getDataConversionError(TIMESTAMP_TZ);
        }
        return ValueTimestampTimeZone.parse(getString().trim());
    }

    private ValueBytes convertToBytes(Mode mode) {
        switch (getValueType()) {
        case JAVA_OBJECT:
        case BLOB:
            return ValueBytes.getNoCopy(getBytesNoCopy());
        case UUID:
        case GEOMETRY:
            return ValueBytes.getNoCopy(getBytes());
        case BYTE:
            return ValueBytes.getNoCopy(new byte[] { getByte() });
        case SHORT: {
            int x = getShort();
            return ValueBytes.getNoCopy(new byte[] { (byte) (x >> 8), (byte) x });
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
            throw getDataConversionError(BYTES);
        }
        String s = getString();
        return ValueBytes.getNoCopy(mode != null && mode.charToBinaryInUtf8 ? s.getBytes(StandardCharsets.UTF_8)
                : StringUtils.convertHexToBytes(s.trim()));
    }

    private ValueString convertToString(Mode mode) {
        String s;
        if (getValueType() == BYTES && mode != null && mode.charToBinaryInUtf8) {
            // Bugfix - Can't use the locale encoding when enabling
            // charToBinaryInUtf8 in mode.
            // The following two target types also are the same issue.
            // @since 2018-07-19 little-pan
            s = new String(getBytesNoCopy(), StandardCharsets.UTF_8);
        } else {
            s = getString();
        }
        return (ValueString) ValueString.get(s);
    }

    private ValueString convertToStringIgnoreCase(Mode mode) {
        String s;
        if (getValueType() == BYTES && mode != null && mode.charToBinaryInUtf8) {
            s = new String(getBytesNoCopy(), StandardCharsets.UTF_8);
        } else {
            s = getString();
        }
        return ValueStringIgnoreCase.get(s);
    }

    private ValueString convertToStringFixed(Mode mode) {
        String s;
        if (getValueType() == BYTES && mode != null && mode.charToBinaryInUtf8) {
            s = new String(getBytesNoCopy(), StandardCharsets.UTF_8);
        } else {
            s = getString();
        }
        return ValueStringFixed.get(s);
    }

    private ValueJavaObject convertToJavaObject() {
        switch (getValueType()) {
        case BYTES:
        case BLOB:
            return ValueJavaObject.getNoCopy(null, getBytesNoCopy(), getDataHandler());
        case ENUM:
        case TIMESTAMP_TZ:
            throw getDataConversionError(JAVA_OBJECT);
        }
        return ValueJavaObject.getNoCopy(null, StringUtils.convertHexToBytes(getString().trim()), getDataHandler());
    }

    private ValueEnum convertToEnumInternal(ExtTypeInfoEnum extTypeInfo) {
        switch (getValueType()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DECIMAL:
            return extTypeInfo.getValue(getInt());
        case STRING:
        case STRING_IGNORECASE:
        case STRING_FIXED:
            return extTypeInfo.getValue(getString());
        case JAVA_OBJECT:
            Object object = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
            if (object instanceof String) {
                return extTypeInfo.getValue((String) object);
            } else if (object instanceof Integer) {
                return extTypeInfo.getValue((int) object);
            }
            //$FALL-THROUGH$
        }
        throw getDataConversionError(ENUM);
    }

    private ValueLobDb convertToBlob() {
        switch (getValueType()) {
        case BYTES:
            return ValueLobDb.createSmallLob(Value.BLOB, getBytesNoCopy());
        case TIMESTAMP_TZ:
            throw getDataConversionError(BLOB);
        }
        return ValueLobDb.createSmallLob(BLOB, StringUtils.convertHexToBytes(getString().trim()));
    }

    private ValueLobDb convertToClob() {
        return ValueLobDb.createSmallLob(CLOB, getString().getBytes(StandardCharsets.UTF_8));
    }

    private ValueUuid convertToUuid() {
        switch (getValueType()) {
        case BYTES:
            return ValueUuid.get(getBytesNoCopy());
        case JAVA_OBJECT:
            Object object = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
            if (object instanceof java.util.UUID) {
                return ValueUuid.get((java.util.UUID) object);
            }
            //$FALL-THROUGH$
        case TIMESTAMP_TZ:
            throw getDataConversionError(UUID);
        }
        return ValueUuid.get(getString());
    }

    private Value convertToGeometry(ExtTypeInfoGeometry extTypeInfo) {
        ValueGeometry result;
        switch (getValueType()) {
        case BYTES:
            result = ValueGeometry.getFromEWKB(getBytesNoCopy());
            break;
        case JAVA_OBJECT:
            Object object = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
            if (DataType.isGeometry(object)) {
                result = ValueGeometry.getFromGeometry(object);
                break;
            }
            //$FALL-THROUGH$
        case TIMESTAMP_TZ:
            throw getDataConversionError(GEOMETRY);
        default:
            result = ValueGeometry.get(getString());
        }
        return extTypeInfo != null ? extTypeInfo.cast(result) : result;
    }

    private ValueInterval convertToIntervalYearMonth(int targetType) {
        switch (getValueType()) {
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED: {
            String s = getString();
            try {
                return (ValueInterval) IntervalUtils
                        .parseFormattedInterval(IntervalQualifier.valueOf(targetType - Value.INTERVAL_YEAR), s)
                        .convertTo(targetType);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "INTERVAL", s);
            }
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_YEAR_TO_MONTH:
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(targetType - Value.INTERVAL_YEAR),
                    IntervalUtils.intervalToAbsolute((ValueInterval) this));
        }
        throw getDataConversionError(targetType);
    }

    private ValueInterval convertToIntervalDayTime(int targetType) {
        switch (getValueType()) {
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED: {
            String s = getString();
            try {
                return (ValueInterval) IntervalUtils
                        .parseFormattedInterval(IntervalQualifier.valueOf(targetType - Value.INTERVAL_YEAR), s)
                        .convertTo(targetType);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "INTERVAL", s);
            }
        }
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(targetType - Value.INTERVAL_YEAR),
                    IntervalUtils.intervalToAbsolute((ValueInterval) this));
        }
        throw getDataConversionError(targetType);
    }

    private ValueArray convertToArray() {
        Value[] a;
        switch (getValueType()) {
        case ROW:
            a = ((ValueRow) this).getList();
            break;
        case BLOB:
        case CLOB:
        case RESULT_SET:
            a = new Value[] { ValueString.get(getString()) };
            break;
        default:
            a = new Value[] { this };
        }
        return ValueArray.get(a);
    }

    private Value convertToRow() {
        Value[] a;
        if (getValueType() == RESULT_SET) {
            ResultInterface result = ((ValueResultSet) this).getResult();
            if (result.hasNext()) {
                a = result.currentRow();
                if (result.hasNext()) {
                    throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
                }
            } else {
                return ValueNull.INSTANCE;
            }
        } else {
            a = new Value[] { this };
        }
        return ValueRow.get(a);
    }

    private ValueResultSet convertToResultSet() {
        SimpleResult result = new SimpleResult();
        result.addColumn("X", "X", getType());
        result.addRow(this);
        return ValueResultSet.get(result);
    }

    /**
     * Creates new instance of the DbException for data conversion error.
     *
     * @param targetType Target data type.
     * @return instance of the DbException.
     */
    DbException getDataConversionError(int targetType) {
        DataType from = DataType.getDataType(getValueType());
        DataType to = DataType.getDataType(targetType);
        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, (from != null ? from.name : "type=" + getValueType())
                + " to " + (to != null ? to.name : "type=" + targetType));
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
    public abstract int compareTypeSafe(Value v, CompareMode mode);

    /**
     * Compare this value against another value using the specified compare
     * mode.
     *
     * @param v the other value
     * @param databaseMode the database mode
     * @param compareMode the compare mode
     * @return 0 if both values are equal, -1 if this value is smaller, and
     *         1 otherwise
     */
    public final int compareTo(Value v, Mode databaseMode, CompareMode compareMode) {
        if (this == v) {
            return 0;
        }
        if (this == ValueNull.INSTANCE) {
            return -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        Value l = this;
        int leftType = l.getValueType();
        int rightType = v.getValueType();
        if (leftType != rightType || leftType == Value.ENUM) {
            int dataType = Value.getHigherOrder(leftType, rightType);
            if (dataType == Value.ENUM) {
                ExtTypeInfoEnum enumerators = ExtTypeInfoEnum.getEnumeratorsForBinaryOperation(l, v);
                l = l.convertToEnum(enumerators);
                v = v.convertToEnum(enumerators);
            } else {
                l = l.convertTo(dataType, databaseMode);
                v = v.convertTo(dataType, databaseMode);
            }
        }
        return l.compareTypeSafe(v, compareMode);
    }

    /**
     * Compare this value against another value using the specified compare
     * mode.
     *
     * @param v the other value
     * @param forEquality perform only check for equality
     * @param databaseMode the database mode
     * @param compareMode the compare mode
     * @return 0 if both values are equal, -1 if this value is smaller, 1
     *         if other value is larger, {@link Integer#MIN_VALUE} if order is
     *         not defined due to NULL comparison
     */
    public int compareWithNull(Value v, boolean forEquality, Mode databaseMode, CompareMode compareMode) {
        if (this == ValueNull.INSTANCE || v == ValueNull.INSTANCE) {
            return Integer.MIN_VALUE;
        }
        Value l = this;
        int leftType = l.getValueType();
        int rightType = v.getValueType();
        if (leftType != rightType || leftType == Value.ENUM) {
            int dataType = Value.getHigherOrder(leftType, rightType);
            if (dataType == Value.ENUM) {
                ExtTypeInfoEnum enumerators = ExtTypeInfoEnum.getEnumeratorsForBinaryOperation(l, v);
                l = l.convertToEnum(enumerators);
                v = v.convertToEnum(enumerators);
            } else {
                l = l.convertTo(dataType, databaseMode);
                v = v.convertTo(dataType, databaseMode);
            }
        }
        return l.compareTypeSafe(v, compareMode);
    }

    /**
     * Returns true if this value is NULL or contains NULL value.
     *
     * @return true if this value is NULL or contains NULL value
     */
    public boolean containsNull() {
        return false;
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

    /**
     * Convert to integer, throwing exception if out of range.
     *
     * @param x integer value.
     * @param column Column info.
     * @return x
     */
    public static int convertToInt(long x, Object column) {
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
        return x.setScale(0, RoundingMode.HALF_UP).longValue();
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
        return getType().getPrecision() <= precision;
    }

    /**
     * Get a medium size SQL expression for debugging or tracing. If the
     * precision is too large, only a subset of the value is returned.
     *
     * @return the SQL expression
     */
    public String getTraceSQL() {
        return getSQL(new StringBuilder()).toString();
    }

    @Override
    public String toString() {
        return getTraceSQL();
    }

    /**
     * Create an exception meaning the specified operation is not supported for
     * this data type.
     *
     * @param op the operation
     * @return the exception
     */
    protected final DbException getUnsupportedExceptionForOperation(String op) {
        return DbException.getUnsupportedException(
                DataType.getDataType(getValueType()).name + " " + op);
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

    /**
     * Returns result for result set value, or single-row result with this value
     * in column X for other values.
     *
     * @return result
     */
    public ResultInterface getResult() {
        SimpleResult rs = new SimpleResult();
        rs.addColumn("X", "X", getType());
        rs.addRow(this);
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

}
