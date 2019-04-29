/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.rtree.SpatialDataType;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.mvstore.type.DataType;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.result.SortOrder;
import org.h2.store.DataHandler;
import org.h2.util.JdbcUtils;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInt;
import org.h2.value.ValueInterval;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLobDb;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueRow;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;

/**
 * A row type.
 */
public class ValueDataType implements DataType {

    private static final byte NULL = 0;
    private static final byte BYTE = 2;
    private static final byte SHORT = 3;
    private static final byte INT = 4;
    private static final byte LONG = 5;
    private static final byte DECIMAL = 6;
    private static final byte DOUBLE = 7;
    private static final byte FLOAT = 8;
    private static final byte TIME = 9;
    private static final byte DATE = 10;
    private static final byte TIMESTAMP = 11;
    private static final byte BYTES = 12;
    private static final byte STRING = 13;
    private static final byte STRING_IGNORECASE = 14;
    private static final byte BLOB = 15;
    private static final byte CLOB = 16;
    private static final byte ARRAY = 17;
    private static final byte RESULT_SET = 18;
    private static final byte JAVA_OBJECT = 19;
    private static final byte UUID = 20;
    private static final byte STRING_FIXED = 21;
    private static final byte GEOMETRY = 22;
    private static final byte TIMESTAMP_TZ = 24;
    private static final byte ENUM = 25;
    private static final byte INTERVAL = 26;
    private static final byte ROW = 27;
    private static final byte INT_0_15 = 32;
    private static final byte LONG_0_7 = 48;
    private static final byte DECIMAL_0_1 = 56;
    private static final byte DECIMAL_SMALL_0 = 58;
    private static final byte DECIMAL_SMALL = 59;
    private static final byte DOUBLE_0_1 = 60;
    private static final byte FLOAT_0_1 = 62;
    private static final byte BOOLEAN_FALSE = 64;
    private static final byte BOOLEAN_TRUE = 65;
    private static final byte INT_NEG = 66;
    private static final byte LONG_NEG = 67;
    private static final byte STRING_0_31 = 68;
    private static final int BYTES_0_31 = 100;
    private static final int SPATIAL_KEY_2D = 132;
    private static final int CUSTOM_DATA_TYPE = 133;

    final DataHandler handler;
    final CompareMode compareMode;
    protected final Mode mode;
    final int[] sortTypes;
    SpatialDataType spatialType;

    public ValueDataType() {
        this(CompareMode.getInstance(null, 0), null, null, null);
    }

    public ValueDataType(Database database, int[] sortTypes) {
        this(database.getCompareMode(), database.getMode(), database, sortTypes);
    }

    private ValueDataType(CompareMode compareMode, Mode mode, DataHandler handler,
            int[] sortTypes) {
        this.compareMode = compareMode;
        this.mode = mode;
        this.handler = handler;
        this.sortTypes = sortTypes;
    }

    private SpatialDataType getSpatialDataType() {
        if (spatialType == null) {
            spatialType = new SpatialDataType(2);
        }
        return spatialType;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a instanceof ValueCollectionBase && b instanceof ValueCollectionBase) {
            Value[] ax = ((ValueCollectionBase) a).getList();
            Value[] bx = ((ValueCollectionBase) b).getList();
            int al = ax.length;
            int bl = bx.length;
            int len = Math.min(al, bl);
            for (int i = 0; i < len; i++) {
                int sortType = sortTypes == null ? SortOrder.ASCENDING : sortTypes[i];
                Value one = ax[i];
                Value two = bx[i];
                if (one == null || two == null) {
                    return compareValues(ax[len - 1], bx[len - 1], SortOrder.ASCENDING);
                }

                int comp = compareValues(one, two, sortType);
                if (comp != 0) {
                    return comp;
                }
            }
            if (len < al) {
                return -1;
            } else if (len < bl) {
                return 1;
            }
            return 0;
        }
        return compareValues((Value) a, (Value) b, SortOrder.ASCENDING);
    }

    private int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        boolean aNull = a == ValueNull.INSTANCE;
        if (aNull || b == ValueNull.INSTANCE) {
            return SortOrder.compareNull(aNull, sortType);
        }

        int comp = a.compareTo(b, mode, compareMode);

        if ((sortType & SortOrder.DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }

    @Override
    public int getMemory(Object obj) {
        if (obj instanceof SpatialKey) {
            return getSpatialDataType().getMemory(obj);
        }
        return getMemory((Value) obj);
    }

    private static int getMemory(Value v) {
        return v == null ? 0 : v.getMemory();
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        return readValue(buff);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        if (obj instanceof SpatialKey) {
            buff.put((byte) SPATIAL_KEY_2D);
            getSpatialDataType().write(buff, obj);
            return;
        }
        Value x = (Value) obj;
        writeValue(buff, x);
    }

    private void writeValue(WriteBuffer buff, Value v) {
        if (v == ValueNull.INSTANCE) {
            buff.put((byte) 0);
            return;
        }
        int type = v.getValueType();
        switch (type) {
        case Value.BOOLEAN:
            buff.put(v.getBoolean() ? BOOLEAN_TRUE : BOOLEAN_FALSE);
            break;
        case Value.BYTE:
            buff.put(BYTE).put(v.getByte());
            break;
        case Value.SHORT:
            buff.put(SHORT).putShort(v.getShort());
            break;
        case Value.ENUM:
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                buff.put(INT_NEG).putVarInt(-x);
            } else if (x < 16) {
                buff.put((byte) (INT_0_15 + x));
            } else {
                buff.put(type == Value.INT ? INT : ENUM).putVarInt(x);
            }
            break;
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                buff.put(LONG_NEG).putVarLong(-x);
            } else if (x < 8) {
                buff.put((byte) (LONG_0_7 + x));
            } else {
                buff.put(LONG).putVarLong(x);
            }
            break;
        }
        case Value.DECIMAL: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                buff.put(DECIMAL_0_1);
            } else if (BigDecimal.ONE.equals(x)) {
                buff.put((byte) (DECIMAL_0_1 + 1));
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits <= 63) {
                    if (scale == 0) {
                        buff.put(DECIMAL_SMALL_0).
                            putVarLong(b.longValue());
                    } else {
                        buff.put(DECIMAL_SMALL).
                            putVarInt(scale).
                            putVarLong(b.longValue());
                    }
                } else {
                    byte[] bytes = b.toByteArray();
                    buff.put(DECIMAL).
                        putVarInt(scale).
                        putVarInt(bytes.length).
                        put(bytes);
                }
            }
            break;
        }
        case Value.TIME: {
            ValueTime t = (ValueTime) v;
            long nanos = t.getNanos();
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put(TIME).
                putVarLong(millis).
                putVarLong(nanos);
            break;
        }
        case Value.DATE: {
            long x = ((ValueDate) v).getDateValue();
            buff.put(DATE).putVarLong(x);
            break;
        }
        case Value.TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getTimeNanos();
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put(TIMESTAMP).
                putVarLong(dateValue).
                putVarLong(millis).
                putVarLong(nanos);
            break;
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getTimeNanos();
            long millis = nanos / 1000000;
            nanos -= millis * 1000000;
            buff.put(TIMESTAMP_TZ).
                putVarLong(dateValue).
                putVarLong(millis).
                putVarLong(nanos).
                putVarInt(ts.getTimeZoneOffsetMins());
            break;
        }
        case Value.JAVA_OBJECT: {
            byte[] b = v.getBytesNoCopy();
            buff.put(JAVA_OBJECT).
                putVarInt(b.length).
                put(b);
            break;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                buff.put((byte) (BYTES_0_31 + len)).
                    put(b);
            } else {
                buff.put(BYTES).
                    putVarInt(b.length).
                    put(b);
            }
            break;
        }
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            buff.put(UUID).
                putLong(uuid.getHigh()).
                putLong(uuid.getLow());
            break;
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                buff.put((byte) (STRING_0_31 + len)).
                    putStringData(s, len);
            } else {
                buff.put(STRING);
                writeString(buff, s);
            }
            break;
        }
        case Value.STRING_IGNORECASE:
            buff.put(STRING_IGNORECASE);
            writeString(buff, v.getString());
            break;
        case Value.STRING_FIXED:
            buff.put(STRING_FIXED);
            writeString(buff, v.getString());
            break;
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 1.0d) {
                buff.put((byte) (DOUBLE_0_1 + 1));
            } else {
                long d = Double.doubleToLongBits(x);
                if (d == ValueDouble.ZERO_BITS) {
                    buff.put(DOUBLE_0_1);
                } else {
                    buff.put(DOUBLE).
                        putVarLong(Long.reverse(d));
                }
            }
            break;
        }
        case Value.FLOAT: {
            float x = v.getFloat();
            if (x == 1.0f) {
                buff.put((byte) (FLOAT_0_1 + 1));
            } else {
                int f = Float.floatToIntBits(x);
                if (f == ValueFloat.ZERO_BITS) {
                    buff.put(FLOAT_0_1);
                } else {
                    buff.put(FLOAT).
                        putVarInt(Integer.reverse(f));
                }
            }
            break;
        }
        case Value.BLOB:
        case Value.CLOB: {
            buff.put(type == Value.BLOB ? BLOB : CLOB);
            ValueLobDb lob = (ValueLobDb) v;
            byte[] small = lob.getSmall();
            if (small == null) {
                buff.putVarInt(-3).
                    putVarInt(lob.getTableId()).
                    putVarLong(lob.getLobId()).
                    putVarLong(lob.getType().getPrecision());
            } else {
                buff.putVarInt(small.length).
                    put(small);
            }
            break;
        }
        case Value.ARRAY:
        case Value.ROW: {
            Value[] list = ((ValueCollectionBase) v).getList();
            buff.put(type == Value.ARRAY ? ARRAY : ROW)
                    .putVarInt(list.length);
            for (Value x : list) {
                writeValue(buff, x);
            }
            break;
        }
        case Value.RESULT_SET: {
            buff.put(RESULT_SET);
            ResultInterface result = ((ValueResultSet) v).getResult();
            int columnCount = result.getVisibleColumnCount();
            buff.putVarInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                writeString(buff, result.getAlias(i));
                writeString(buff, result.getColumnName(i));
                TypeInfo columnType = result.getColumnType(i);
                buff.putVarInt(columnType.getValueType()).
                    putVarLong(columnType.getPrecision()).
                    putVarInt(columnType.getScale());
            }
            while (result.next()) {
                buff.put((byte) 1);
                Value[] row = result.currentRow();
                for (int i = 0; i < columnCount; i++) {
                    writeValue(buff, row[i]);
                }
            }
            buff.put((byte) 0);
            break;
        }
        case Value.GEOMETRY: {
            byte[] b = v.getBytes();
            int len = b.length;
            buff.put(GEOMETRY).
                putVarInt(len).
                put(b);
            break;
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE: {
            ValueInterval interval = (ValueInterval) v;
            int ordinal = type - Value.INTERVAL_YEAR;
            if (interval.isNegative()) {
                ordinal = ~ordinal;
            }
            buff.put(INTERVAL).
                put((byte) ordinal).
                putVarLong(interval.getLeading());
            break;
        }
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND: {
            ValueInterval interval = (ValueInterval) v;
            int ordinal = type - Value.INTERVAL_YEAR;
            if (interval.isNegative()) {
                ordinal = ~ordinal;
            }
            buff.put(INTERVAL).
                put((byte) (ordinal)).
                putVarLong(interval.getLeading()).
                putVarLong(interval.getRemaining());
            break;
        }
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                byte[] b = v.getBytesNoCopy();
                buff.put((byte)CUSTOM_DATA_TYPE).
                    putVarInt(type).
                    putVarInt(b.length).
                    put(b);
                break;
            }
            DbException.throwInternalError("type=" + v.getValueType());
        }
    }

    private static void writeString(WriteBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    private Object readValue(ByteBuffer buff) {
        int type = buff.get() & 255;
        switch (type) {
        case NULL:
            return ValueNull.INSTANCE;
        case BOOLEAN_TRUE:
            return ValueBoolean.TRUE;
        case BOOLEAN_FALSE:
            return ValueBoolean.FALSE;
        case INT_NEG:
            return ValueInt.get(-readVarInt(buff));
        case ENUM:
        case INT:
            return ValueInt.get(readVarInt(buff));
        case LONG_NEG:
            return ValueLong.get(-readVarLong(buff));
        case LONG:
            return ValueLong.get(readVarLong(buff));
        case BYTE:
            return ValueByte.get(buff.get());
        case SHORT:
            return ValueShort.get(buff.getShort());
        case DECIMAL_0_1:
            return ValueDecimal.ZERO;
        case DECIMAL_0_1 + 1:
            return ValueDecimal.ONE;
        case DECIMAL_SMALL_0:
            return ValueDecimal.get(BigDecimal.valueOf(
                    readVarLong(buff)));
        case DECIMAL_SMALL: {
            int scale = readVarInt(buff);
            return ValueDecimal.get(BigDecimal.valueOf(
                    readVarLong(buff), scale));
        }
        case DECIMAL: {
            int scale = readVarInt(buff);
            int len = readVarInt(buff);
            byte[] buff2 = Utils.newBytes(len);
            buff.get(buff2, 0, len);
            BigInteger b = new BigInteger(buff2);
            return ValueDecimal.get(new BigDecimal(b, scale));
        }
        case DATE: {
            return ValueDate.fromDateValue(readVarLong(buff));
        }
        case TIME: {
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            return ValueTime.fromNanos(nanos);
        }
        case TIMESTAMP: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
        }
        case TIMESTAMP_TZ: {
            long dateValue = readVarLong(buff);
            long nanos = readVarLong(buff) * 1000000 + readVarLong(buff);
            short tz = (short) readVarInt(buff);
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tz);
        }
        case BYTES: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueBytes.getNoCopy(b);
        }
        case JAVA_OBJECT: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueJavaObject.getNoCopy(null, b, handler);
        }
        case UUID:
            return ValueUuid.get(buff.getLong(), buff.getLong());
        case STRING:
            return ValueString.get(readString(buff));
        case STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString(buff));
        case STRING_FIXED:
            return ValueStringFixed.get(readString(buff));
        case INTERVAL: {
            int ordinal = buff.get();
            boolean negative = ordinal < 0;
            if (negative) {
                ordinal = ~ordinal;
            }
            return ValueInterval.from(IntervalQualifier.valueOf(ordinal), negative, readVarLong(buff),
                    ordinal < 5 ? 0 : readVarLong(buff));
        }
        case FLOAT_0_1:
            return ValueFloat.ZERO;
        case FLOAT_0_1 + 1:
            return ValueFloat.ONE;
        case DOUBLE_0_1:
            return ValueDouble.ZERO;
        case DOUBLE_0_1 + 1:
            return ValueDouble.ONE;
        case DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(Long.reverse(readVarLong(buff))));
        case FLOAT:
            return ValueFloat.get(Float.intBitsToFloat(Integer.reverse(readVarInt(buff))));
        case BLOB:
        case CLOB: {
            int smallLen = readVarInt(buff);
            if (smallLen >= 0) {
                byte[] small = Utils.newBytes(smallLen);
                buff.get(small, 0, smallLen);
                return ValueLobDb.createSmallLob(type == BLOB ? Value.BLOB : Value.CLOB, small);
            } else if (smallLen == -3) {
                int tableId = readVarInt(buff);
                long lobId = readVarLong(buff);
                long precision = readVarLong(buff);
                return ValueLobDb.create(type == BLOB ? Value.BLOB : Value.CLOB,
                        handler, tableId, lobId, null, precision);
            } else {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                        "lob type: " + smallLen);
            }
        }
        case ARRAY:
        case ROW: {
            int len = readVarInt(buff);
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = (Value) readValue(buff);
            }
            return type == ARRAY ? ValueArray.get(list) : ValueRow.get(list);
        }
        case RESULT_SET: {
            SimpleResult rs = new SimpleResult();
            int columns = readVarInt(buff);
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(buff), readString(buff), readVarInt(buff), readVarLong(buff),
                        readVarInt(buff));
            }
            while (buff.get() != 0) {
                Value[] o = new Value[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = (Value) readValue(buff);
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case GEOMETRY: {
            int len = readVarInt(buff);
            byte[] b = Utils.newBytes(len);
            buff.get(b, 0, len);
            return ValueGeometry.get(b);
        }
        case SPATIAL_KEY_2D:
            return getSpatialDataType().read(buff);
        case CUSTOM_DATA_TYPE: {
            if (JdbcUtils.customDataTypesHandler != null) {
                int customType = readVarInt(buff);
                int len = readVarInt(buff);
                byte[] b = Utils.newBytes(len);
                buff.get(b, 0, len);
                return JdbcUtils.customDataTypesHandler.convert(
                        ValueBytes.getNoCopy(b), customType);
            }
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                    "No CustomDataTypesHandler has been set up");
        }
        default:
            if (type >= INT_0_15 && type < INT_0_15 + 16) {
                return ValueInt.get(type - INT_0_15);
            } else if (type >= LONG_0_7 && type < LONG_0_7 + 8) {
                return ValueLong.get(type - LONG_0_7);
            } else if (type >= BYTES_0_31 && type < BYTES_0_31 + 32) {
                int len = type - BYTES_0_31;
                byte[] b = Utils.newBytes(len);
                buff.get(b, 0, len);
                return ValueBytes.getNoCopy(b);
            } else if (type >= STRING_0_31 && type < STRING_0_31 + 32) {
                return ValueString.get(readString(buff, type - STRING_0_31));
            }
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
        }
    }

    private static int readVarInt(ByteBuffer buff) {
        return DataUtils.readVarInt(buff);
    }

    private static long readVarLong(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    private static String readString(ByteBuffer buff, int len) {
        return DataUtils.readString(buff, len);
    }

    private static String readString(ByteBuffer buff) {
        int len = readVarInt(buff);
        return DataUtils.readString(buff, len);
    }

    @Override
    public int hashCode() {
        return compareMode.hashCode() ^ Arrays.hashCode(sortTypes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof ValueDataType)) {
            return false;
        }
        ValueDataType v = (ValueDataType) obj;
        if (!compareMode.equals(v.compareMode)) {
            return false;
        }
        return Arrays.equals(sortTypes, v.sortTypes);
    }

}
