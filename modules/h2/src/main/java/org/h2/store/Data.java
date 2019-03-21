/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 *
 * The variable size number format code is a port from SQLite,
 * but stored in reverse order (least significant bits in the first byte).
 */
package org.h2.store;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.Utils;
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
import org.h2.value.ValueLob;
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
 * This class represents a byte buffer that contains persistent data of a page.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class Data {

    /**
     * The length of an integer value.
     */
    public static final int LENGTH_INT = 4;

    /**
     * The length of a long value.
     */
    public static final int LENGTH_LONG = 8;

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
    private static final int LOCAL_TIME = 132;
    private static final int LOCAL_DATE = 133;
    private static final int LOCAL_TIMESTAMP = 134;
    private static final int CUSTOM_DATA_TYPE = 135;

    private static final long MILLIS_PER_MINUTE = 1000 * 60;

    /**
     * The data itself.
     */
    private byte[] data;

    /**
     * The current write or read position.
     */
    private int pos;

    /**
     * The data handler responsible for lob objects.
     */
    private final DataHandler handler;

    private final boolean storeLocalTime;

    private Data(DataHandler handler, byte[] data, boolean storeLocalTime) {
        this.handler = handler;
        this.data = data;
        this.storeLocalTime = storeLocalTime;
    }

    /**
     * Update an integer at the given position.
     * The current position is not change.
     *
     * @param pos the position
     * @param x the value
     */
    public void setInt(int pos, int x) {
        Bits.writeInt(data, pos, x);
    }

    /**
     * Write an integer at the current position.
     * The current position is incremented.
     *
     * @param x the value
     */
    public void writeInt(int x) {
        Bits.writeInt(data, pos, x);
        pos += 4;
    }

    /**
     * Read an integer at the current position.
     * The current position is incremented.
     *
     * @return the value
     */
    public int readInt() {
        int x = Bits.readInt(data, pos);
        pos += 4;
        return x;
    }

    /**
     * Get the length of a String. This includes the bytes required to encode
     * the length.
     *
     * @param s the string
     * @return the number of bytes required
     */
    public static int getStringLen(String s) {
        int len = s.length();
        return getStringWithoutLengthLen(s, len) + getVarIntLen(len);
    }

    /**
     * Calculate the length of String, excluding the bytes required to encode
     * the length.
     * <p>
     * For performance reasons the internal representation of a String is
     * similar to UTF-8, but not exactly UTF-8.
     *
     * @param s the string
     * @param len the length of the string
     * @return the number of bytes required
     */
    private static int getStringWithoutLengthLen(String s, int len) {
        int plus = 0;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c >= 0x800) {
                plus += 2;
            } else if (c >= 0x80) {
                plus++;
            }
        }
        return len + plus;
    }

    /**
     * Read a String value.
     * The current position is incremented.
     *
     * @return the value
     */
    public String readString() {
        int len = readVarInt();
        return readString(len);
    }

    /**
     * Read a String from the byte array.
     * <p>
     * For performance reasons the internal representation of a String is
     * similar to UTF-8, but not exactly UTF-8.
     *
     * @param len the length of the resulting string
     * @return the String
     */
    private String readString(int len) {
        byte[] buff = data;
        int p = pos;
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff[p++] & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) +
                        ((buff[p++] & 0x3f) << 6) +
                        (buff[p++] & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) +
                        (buff[p++] & 0x3f));
            }
        }
        pos = p;
        return new String(chars);
    }

    /**
     * Write a String.
     * The current position is incremented.
     *
     * @param s the value
     */
    public void writeString(String s) {
        int len = s.length();
        writeVarInt(len);
        writeStringWithoutLength(s, len);
    }

    /**
     * Write a String.
     * <p>
     * For performance reasons the internal representation of a String is
     * similar to UTF-8, but not exactly UTF-8.
     *
     * @param s the string
     * @param len the number of characters to write
     */
    private void writeStringWithoutLength(String s, int len) {
        int p = pos;
        byte[] buff = data;
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (((c >> 6) & 0x3f));
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        pos = p;
    }

    private void writeStringWithoutLength(char[] chars, int len) {
        int p = pos;
        byte[] buff = data;
        for (int i = 0; i < len; i++) {
            int c = chars[i];
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (((c >> 6) & 0x3f));
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        pos = p;
    }

    /**
     * Create a new buffer for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param capacity the initial capacity of the buffer
     * @param storeLocalTime
     *            store DATE, TIME, and TIMESTAMP values with local time storage
     *            format
     * @return the buffer
     */
    public static Data create(DataHandler handler, int capacity, boolean storeLocalTime) {
        return new Data(handler, new byte[capacity], storeLocalTime);
    }

    /**
     * Create a new buffer using the given data for the given handler. The
     * handler will decide what type of buffer is created.
     *
     * @param handler the data handler
     * @param buff the data
     * @param storeLocalTime
     *            store DATE, TIME, and TIMESTAMP values with local time storage
     *            format
     * @return the buffer
     */
    public static Data create(DataHandler handler, byte[] buff, boolean storeLocalTime) {
        return new Data(handler, buff, storeLocalTime);
    }

    /**
     * Get the current write position of this buffer, which is the current
     * length.
     *
     * @return the length
     */
    public int length() {
        return pos;
    }

    /**
     * Get the byte array used for this page.
     *
     * @return the byte array
     */
    public byte[] getBytes() {
        return data;
    }

    /**
     * Set the position to 0.
     */
    public void reset() {
        pos = 0;
    }

    /**
     * Append a number of bytes to this buffer.
     *
     * @param buff the data
     * @param off the offset in the data
     * @param len the length in bytes
     */
    public void write(byte[] buff, int off, int len) {
        System.arraycopy(buff, off, data, pos, len);
        pos += len;
    }

    /**
     * Copy a number of bytes to the given buffer from the current position. The
     * current position is incremented accordingly.
     *
     * @param buff the output buffer
     * @param off the offset in the output buffer
     * @param len the number of bytes to copy
     */
    public void read(byte[] buff, int off, int len) {
        System.arraycopy(data, pos, buff, off, len);
        pos += len;
    }

    /**
     * Append one single byte.
     *
     * @param x the value
     */
    public void writeByte(byte x) {
        data[pos++] = x;
    }

    /**
     * Read one single byte.
     *
     * @return the value
     */
    public byte readByte() {
        return data[pos++];
    }

    /**
     * Read a long value. This method reads two int values and combines them.
     *
     * @return the long value
     */
    public long readLong() {
        long x = Bits.readLong(data, pos);
        pos += 8;
        return x;
    }

    /**
     * Append a long value. This method writes two int values.
     *
     * @param x the value
     */
    public void writeLong(long x) {
        Bits.writeLong(data, pos, x);
        pos += 8;
    }

    /**
     * Append a value.
     *
     * @param v the value
     */
    public void writeValue(Value v) {
        int start = pos;
        if (v == ValueNull.INSTANCE) {
            data[pos++] = NULL;
            return;
        }
        int type = v.getValueType();
        switch (type) {
        case Value.BOOLEAN:
            writeByte(v.getBoolean() ? BOOLEAN_TRUE : BOOLEAN_FALSE);
            break;
        case Value.BYTE:
            writeByte(BYTE);
            writeByte(v.getByte());
            break;
        case Value.SHORT:
            writeByte(SHORT);
            writeShortInt(v.getShort());
            break;
        case Value.ENUM:
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                writeByte(INT_NEG);
                writeVarInt(-x);
            } else if (x < 16) {
                writeByte((byte) (INT_0_15 + x));
            } else {
                writeByte(type == Value.INT ? INT : ENUM);
                writeVarInt(x);
            }
            break;
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                writeByte(LONG_NEG);
                writeVarLong(-x);
            } else if (x < 8) {
                writeByte((byte) (LONG_0_7 + x));
            } else {
                writeByte(LONG);
                writeVarLong(x);
            }
            break;
        }
        case Value.DECIMAL: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                writeByte(DECIMAL_0_1);
            } else if (BigDecimal.ONE.equals(x)) {
                writeByte((byte) (DECIMAL_0_1 + 1));
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits <= 63) {
                    if (scale == 0) {
                        writeByte(DECIMAL_SMALL_0);
                        writeVarLong(b.longValue());
                    } else {
                        writeByte(DECIMAL_SMALL);
                        writeVarInt(scale);
                        writeVarLong(b.longValue());
                    }
                } else {
                    writeByte(DECIMAL);
                    writeVarInt(scale);
                    byte[] bytes = b.toByteArray();
                    writeVarInt(bytes.length);
                    write(bytes, 0, bytes.length);
                }
            }
            break;
        }
        case Value.TIME:
            if (storeLocalTime) {
                writeByte((byte) LOCAL_TIME);
                ValueTime t = (ValueTime) v;
                long nanos = t.getNanos();
                long millis = nanos / 1_000_000;
                nanos -= millis * 1_000_000;
                writeVarLong(millis);
                writeVarLong(nanos);
            } else {
                writeByte(TIME);
                writeVarLong(DateTimeUtils.getTimeLocalWithoutDst(v.getTime()));
            }
            break;
        case Value.DATE: {
            if (storeLocalTime) {
                writeByte((byte) LOCAL_DATE);
                long x = ((ValueDate) v).getDateValue();
                writeVarLong(x);
            } else {
                writeByte(DATE);
                long x = DateTimeUtils.getTimeLocalWithoutDst(v.getDate());
                writeVarLong(x / MILLIS_PER_MINUTE);
            }
            break;
        }
        case Value.TIMESTAMP: {
            if (storeLocalTime) {
                writeByte((byte) LOCAL_TIMESTAMP);
                ValueTimestamp ts = (ValueTimestamp) v;
                long dateValue = ts.getDateValue();
                writeVarLong(dateValue);
                long nanos = ts.getTimeNanos();
                long millis = nanos / 1_000_000;
                nanos -= millis * 1_000_000;
                writeVarLong(millis);
                writeVarLong(nanos);
            } else {
                Timestamp ts = v.getTimestamp();
                writeByte(TIMESTAMP);
                writeVarLong(DateTimeUtils.getTimeLocalWithoutDst(ts));
                writeVarInt(ts.getNanos() % 1_000_000);
            }
            break;
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            writeByte(TIMESTAMP_TZ);
            writeVarLong(ts.getDateValue());
            writeVarLong(ts.getTimeNanos());
            writeVarInt(ts.getTimeZoneOffsetMins());
            break;
        }
        case Value.GEOMETRY:
            // fall though
        case Value.JAVA_OBJECT: {
            writeByte(type == Value.GEOMETRY ? GEOMETRY : JAVA_OBJECT);
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            writeVarInt(len);
            write(b, 0, len);
            break;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                writeByte((byte) (BYTES_0_31 + len));
                write(b, 0, len);
            } else {
                writeByte(BYTES);
                writeVarInt(len);
                write(b, 0, len);
            }
            break;
        }
        case Value.UUID: {
            writeByte(UUID);
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                writeByte((byte) (STRING_0_31 + len));
                writeStringWithoutLength(s, len);
            } else {
                writeByte(STRING);
                writeString(s);
            }
            break;
        }
        case Value.STRING_IGNORECASE:
            writeByte(STRING_IGNORECASE);
            writeString(v.getString());
            break;
        case Value.STRING_FIXED:
            writeByte(STRING_FIXED);
            writeString(v.getString());
            break;
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 1.0d) {
                writeByte((byte) (DOUBLE_0_1 + 1));
            } else {
                long d = Double.doubleToLongBits(x);
                if (d == ValueDouble.ZERO_BITS) {
                    writeByte(DOUBLE_0_1);
                } else {
                    writeByte(DOUBLE);
                    writeVarLong(Long.reverse(d));
                }
            }
            break;
        }
        case Value.FLOAT: {
            float x = v.getFloat();
            if (x == 1.0f) {
                writeByte((byte) (FLOAT_0_1 + 1));
            } else {
                int f = Float.floatToIntBits(x);
                if (f == ValueFloat.ZERO_BITS) {
                    writeByte(FLOAT_0_1);
                } else {
                    writeByte(FLOAT);
                    writeVarInt(Integer.reverse(f));
                }
            }
            break;
        }
        case Value.BLOB:
        case Value.CLOB: {
            writeByte(type == Value.BLOB ? BLOB : CLOB);
            if (v instanceof ValueLob) {
                ValueLob lob = (ValueLob) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    int t = -1;
                    if (!lob.isLinkedToTable()) {
                        t = -2;
                    }
                    writeVarInt(t);
                    writeVarInt(lob.getTableId());
                    writeVarInt(lob.getObjectId());
                    writeVarLong(lob.getType().getPrecision());
                    writeByte((byte) (lob.isCompressed() ? 1 : 0));
                    if (t == -2) {
                        writeString(lob.getFileName());
                    }
                } else {
                    writeVarInt(small.length);
                    write(small, 0, small.length);
                }
            } else {
                ValueLobDb lob = (ValueLobDb) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    writeVarInt(-3);
                    writeVarInt(lob.getTableId());
                    writeVarLong(lob.getLobId());
                    writeVarLong(lob.getType().getPrecision());
                } else {
                    writeVarInt(small.length);
                    write(small, 0, small.length);
                }
            }
            break;
        }
        case Value.ARRAY:
        case Value.ROW: {
            writeByte(type == Value.ARRAY ? ARRAY : ROW);
            Value[] list = ((ValueCollectionBase) v).getList();
            writeVarInt(list.length);
            for (Value x : list) {
                writeValue(x);
            }
            break;
        }
        case Value.RESULT_SET: {
            writeByte(RESULT_SET);
            ResultInterface result = ((ValueResultSet) v).getResult();
            result.reset();
            int columnCount = result.getVisibleColumnCount();
            writeVarInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                writeString(result.getAlias(i));
                writeString(result.getColumnName(i));
                TypeInfo columnType = result.getColumnType(i);
                writeVarInt(columnType.getValueType());
                writeVarLong(columnType.getPrecision());
                writeVarInt(columnType.getScale());
            }
            while (result.next()) {
                writeByte((byte) 1);
                Value[] row = result.currentRow();
                for (int i = 0; i < columnCount; i++) {
                    writeValue(row[i]);
                }
            }
            writeByte((byte) 0);
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
            writeByte(INTERVAL);
            writeByte((byte) ordinal);
            writeVarLong(interval.getLeading());
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
            writeByte(INTERVAL);
            writeByte((byte) ordinal);
            writeVarLong(interval.getLeading());
            writeVarLong(interval.getRemaining());
            break;
        }
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                byte[] b = v.getBytesNoCopy();
                writeByte((byte) CUSTOM_DATA_TYPE);
                writeVarInt(type);
                writeVarInt(b.length);
                write(b, 0, b.length);
                break;
            }
            DbException.throwInternalError("type=" + v.getValueType());
        }
        assert pos - start == getValueLen(v)
                : "value size error: got " + (pos - start) + " expected " + getValueLen(v);
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public Value readValue() {
        int type = data[pos++] & 255;
        switch (type) {
        case NULL:
            return ValueNull.INSTANCE;
        case BOOLEAN_TRUE:
            return ValueBoolean.TRUE;
        case BOOLEAN_FALSE:
            return ValueBoolean.FALSE;
        case INT_NEG:
            return ValueInt.get(-readVarInt());
        case ENUM:
        case INT:
            return ValueInt.get(readVarInt());
        case LONG_NEG:
            return ValueLong.get(-readVarLong());
        case Value.LONG:
            return ValueLong.get(readVarLong());
        case BYTE:
            return ValueByte.get(readByte());
        case SHORT:
            return ValueShort.get(readShortInt());
        case DECIMAL_0_1:
            return (ValueDecimal) ValueDecimal.ZERO;
        case DECIMAL_0_1 + 1:
            return (ValueDecimal) ValueDecimal.ONE;
        case DECIMAL_SMALL_0:
            return ValueDecimal.get(BigDecimal.valueOf(readVarLong()));
        case DECIMAL_SMALL: {
            int scale = readVarInt();
            return ValueDecimal.get(BigDecimal.valueOf(readVarLong(), scale));
        }
        case DECIMAL: {
            int scale = readVarInt();
            int len = readVarInt();
            byte[] buff = Utils.newBytes(len);
            read(buff, 0, len);
            BigInteger b = new BigInteger(buff);
            return ValueDecimal.get(new BigDecimal(b, scale));
        }
        case LOCAL_DATE: {
            return ValueDate.fromDateValue(readVarLong());
        }
        case DATE: {
            long x = readVarLong() * MILLIS_PER_MINUTE;
            return ValueDate.fromMillis(DateTimeUtils.getTimeUTCWithoutDst(x));
        }
        case LOCAL_TIME: {
            long nanos = readVarLong() * 1_000_000 + readVarLong();
            return ValueTime.fromNanos(nanos);
        }
        case TIME:
            // need to normalize the year, month and day
            return ValueTime.fromMillis(
                    DateTimeUtils.getTimeUTCWithoutDst(readVarLong()));
        case LOCAL_TIMESTAMP: {
            long dateValue = readVarLong();
            long nanos = readVarLong() * 1_000_000 + readVarLong();
            return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
        }
        case TIMESTAMP: {
            return ValueTimestamp.fromMillisNanos(
                    DateTimeUtils.getTimeUTCWithoutDst(readVarLong()),
                    readVarInt() % 1_000_000);
        }
        case TIMESTAMP_TZ: {
            long dateValue = readVarLong();
            long nanos = readVarLong();
            short tz = (short) readVarInt();
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tz);
        }
        case BYTES: {
            int len = readVarInt();
            byte[] b = Utils.newBytes(len);
            read(b, 0, len);
            return ValueBytes.getNoCopy(b);
        }
        case GEOMETRY: {
            int len = readVarInt();
            byte[] b = Utils.newBytes(len);
            read(b, 0, len);
            return ValueGeometry.get(b);
        }
        case JAVA_OBJECT: {
            int len = readVarInt();
            byte[] b = Utils.newBytes(len);
            read(b, 0, len);
            return ValueJavaObject.getNoCopy(null, b, handler);
        }
        case UUID:
            return ValueUuid.get(readLong(), readLong());
        case STRING:
            return ValueString.get(readString());
        case STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case STRING_FIXED:
            return ValueStringFixed.get(readString());
        case FLOAT_0_1:
            return ValueFloat.ZERO;
        case FLOAT_0_1 + 1:
            return ValueFloat.ONE;
        case DOUBLE_0_1:
            return ValueDouble.ZERO;
        case DOUBLE_0_1 + 1:
            return ValueDouble.ONE;
        case DOUBLE:
            return ValueDouble.get(Double.longBitsToDouble(Long.reverse(readVarLong())));
        case FLOAT:
            return ValueFloat.get(Float.intBitsToFloat(Integer.reverse(readVarInt())));
        case BLOB:
        case CLOB: {
            int smallLen = readVarInt();
            if (smallLen >= 0) {
                byte[] small = Utils.newBytes(smallLen);
                read(small, 0, smallLen);
                return ValueLobDb.createSmallLob(type == BLOB ? Value.BLOB : Value.CLOB, small);
            } else if (smallLen == -3) {
                int tableId = readVarInt();
                long lobId = readVarLong();
                long precision = readVarLong();
                return ValueLobDb.create(type == BLOB ? Value.BLOB : Value.CLOB, handler, tableId,
                        lobId, null, precision);
            } else {
                int tableId = readVarInt();
                int objectId = readVarInt();
                long precision = 0;
                boolean compression = false;
                // -1: regular; -2: regular, but not linked (in this case:
                // including file name)
                if (smallLen == -1 || smallLen == -2) {
                    precision = readVarLong();
                    compression = readByte() == 1;
                }
                if (smallLen == -2) {
                    String filename = readString();
                    return ValueLob.openUnlinked(type == BLOB ? Value.BLOB : Value.CLOB, handler, tableId,
                            objectId, precision, compression, filename);
                }
                return ValueLob.openLinked(type == BLOB ? Value.BLOB : Value.CLOB, handler, tableId,
                        objectId, precision, compression);
            }
        }
        case ARRAY:
        case ROW: // Special storage type for ValueRow
        {
            int len = readVarInt();
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return type == ARRAY ? ValueArray.get(list) : ValueRow.get(list);
        }
        case RESULT_SET: {
            SimpleResult rs = new SimpleResult();
            int columns = readVarInt();
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(), readString(), readVarInt(), readVarLong(), readVarInt());
            }
            while (readByte() != 0) {
                Value[] o = new Value[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case INTERVAL: {
            int ordinal = readByte();
            boolean negative = ordinal < 0;
            if (negative) {
                ordinal = ~ordinal;
            }
            return ValueInterval.from(IntervalQualifier.valueOf(ordinal), negative, readVarLong(),
                    ordinal < 5 ? 0 : readVarLong());
        }
        case CUSTOM_DATA_TYPE: {
            if (JdbcUtils.customDataTypesHandler != null) {
                int customType = readVarInt();
                int len = readVarInt();
                byte[] b = Utils.newBytes(len);
                read(b, 0, len);
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
                read(b, 0, len);
                return ValueBytes.getNoCopy(b);
            } else if (type >= STRING_0_31 && type < STRING_0_31 + 32) {
                return ValueString.get(readString(type - STRING_0_31));
            }
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "type: " + type);
        }
    }

    /**
     * Calculate the number of bytes required to encode the given value.
     *
     * @param v the value
     * @return the number of bytes required to store this value
     */
    public int getValueLen(Value v) {
        return getValueLen(v, storeLocalTime);
    }

    /**
     * Calculate the number of bytes required to encode the given value.
     *
     * @param v the value
     * @param storeLocalTime
     *            calculate size of DATE, TIME, and TIMESTAMP values with local
     *            time storage format
     * @return the number of bytes required to store this value
     */
    public static int getValueLen(Value v, boolean storeLocalTime) {
        if (v == ValueNull.INSTANCE) {
            return 1;
        }
        switch (v.getValueType()) {
        case Value.BOOLEAN:
            return 1;
        case Value.BYTE:
            return 2;
        case Value.SHORT:
            return 3;
        case Value.ENUM:
        case Value.INT: {
            int x = v.getInt();
            if (x < 0) {
                return 1 + getVarIntLen(-x);
            } else if (x < 16) {
                return 1;
            } else {
                return 1 + getVarIntLen(x);
            }
        }
        case Value.LONG: {
            long x = v.getLong();
            if (x < 0) {
                return 1 + getVarLongLen(-x);
            } else if (x < 8) {
                return 1;
            } else {
                return 1 + getVarLongLen(x);
            }
        }
        case Value.DOUBLE: {
            double x = v.getDouble();
            if (x == 1.0d) {
                return 1;
            }
            long d = Double.doubleToLongBits(x);
            if (d == ValueDouble.ZERO_BITS) {
                return 1;
            }
            return 1 + getVarLongLen(Long.reverse(d));
        }
        case Value.FLOAT: {
            float x = v.getFloat();
            if (x == 1.0f) {
                return 1;
            }
            int f = Float.floatToIntBits(x);
            if (f == ValueFloat.ZERO_BITS) {
                return 1;
            }
            return 1 + getVarIntLen(Integer.reverse(f));
        }
        case Value.STRING: {
            String s = v.getString();
            int len = s.length();
            if (len < 32) {
                return 1 + getStringWithoutLengthLen(s, len);
            }
            return 1 + getStringLen(s);
        }
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            return 1 + getStringLen(v.getString());
        case Value.DECIMAL: {
            BigDecimal x = v.getBigDecimal();
            if (BigDecimal.ZERO.equals(x)) {
                return 1;
            } else if (BigDecimal.ONE.equals(x)) {
                return 1;
            }
            int scale = x.scale();
            BigInteger b = x.unscaledValue();
            int bits = b.bitLength();
            if (bits <= 63) {
                if (scale == 0) {
                    return 1 + getVarLongLen(b.longValue());
                }
                return 1 + getVarIntLen(scale) + getVarLongLen(b.longValue());
            }
            byte[] bytes = b.toByteArray();
            return 1 + getVarIntLen(scale) + getVarIntLen(bytes.length) + bytes.length;
        }
        case Value.TIME:
            if (storeLocalTime) {
                long nanos = ((ValueTime) v).getNanos();
                long millis = nanos / 1_000_000;
                nanos -= millis * 1_000_000;
                return 1 + getVarLongLen(millis) + getVarLongLen(nanos);
            }
            return 1 + getVarLongLen(DateTimeUtils.getTimeLocalWithoutDst(v.getTime()));
        case Value.DATE: {
            if (storeLocalTime) {
                long dateValue = ((ValueDate) v).getDateValue();
                return 1 + getVarLongLen(dateValue);
            }
            long x = DateTimeUtils.getTimeLocalWithoutDst(v.getDate());
            return 1 + getVarLongLen(x / MILLIS_PER_MINUTE);
        }
        case Value.TIMESTAMP: {
            if (storeLocalTime) {
                ValueTimestamp ts = (ValueTimestamp) v;
                long dateValue = ts.getDateValue();
                long nanos = ts.getTimeNanos();
                long millis = nanos / 1_000_000;
                nanos -= millis * 1_000_000;
                return 1 + getVarLongLen(dateValue) + getVarLongLen(millis) +
                        getVarLongLen(nanos);
            }
            Timestamp ts = v.getTimestamp();
            return 1 + getVarLongLen(DateTimeUtils.getTimeLocalWithoutDst(ts)) +
                    getVarIntLen(ts.getNanos() % 1_000_000);
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            long dateValue = ts.getDateValue();
            long nanos = ts.getTimeNanos();
            short tz = ts.getTimeZoneOffsetMins();
            return 1 + getVarLongLen(dateValue) + getVarLongLen(nanos) +
                    getVarIntLen(tz);
        }
        case Value.GEOMETRY:
        case Value.JAVA_OBJECT: {
            byte[] b = v.getBytesNoCopy();
            return 1 + getVarIntLen(b.length) + b.length;
        }
        case Value.BYTES: {
            byte[] b = v.getBytesNoCopy();
            int len = b.length;
            if (len < 32) {
                return 1 + b.length;
            }
            return 1 + getVarIntLen(b.length) + b.length;
        }
        case Value.UUID:
            return 1 + LENGTH_LONG + LENGTH_LONG;
        case Value.BLOB:
        case Value.CLOB: {
            int len = 1;
            if (v instanceof ValueLob) {
                ValueLob lob = (ValueLob) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    int t = -1;
                    if (!lob.isLinkedToTable()) {
                        t = -2;
                    }
                    len += getVarIntLen(t);
                    len += getVarIntLen(lob.getTableId());
                    len += getVarIntLen(lob.getObjectId());
                    len += getVarLongLen(lob.getType().getPrecision());
                    len += 1;
                    if (t == -2) {
                        len += getStringLen(lob.getFileName());
                    }
                } else {
                    len += getVarIntLen(small.length);
                    len += small.length;
                }
            } else {
                ValueLobDb lob = (ValueLobDb) v;
                byte[] small = lob.getSmall();
                if (small == null) {
                    len += getVarIntLen(-3);
                    len += getVarIntLen(lob.getTableId());
                    len += getVarLongLen(lob.getLobId());
                    len += getVarLongLen(lob.getType().getPrecision());
                } else {
                    len += getVarIntLen(small.length);
                    len += small.length;
                }
            }
            return len;
        }
        case Value.ARRAY:
        case Value.ROW: {
            Value[] list = ((ValueCollectionBase) v).getList();
            int len = 1 + getVarIntLen(list.length);
            for (Value x : list) {
                len += getValueLen(x, storeLocalTime);
            }
            return len;
        }
        case Value.RESULT_SET: {
            int len = 1;
            ResultInterface result = ((ValueResultSet) v).getResult();
            int columnCount = result.getVisibleColumnCount();
            len += getVarIntLen(columnCount);
            for (int i = 0; i < columnCount; i++) {
                len += getStringLen(result.getAlias(i));
                len += getStringLen(result.getColumnName(i));
                TypeInfo columnType = result.getColumnType(i);
                len += getVarIntLen(columnType.getValueType());
                len += getVarLongLen(columnType.getPrecision());
                len += getVarIntLen(columnType.getScale());
            }
            while (result.next()) {
                len++;
                Value[] row = result.currentRow();
                for (int i = 0; i < columnCount; i++) {
                    Value val = row[i];
                    len += getValueLen(val, storeLocalTime);
                }
            }
            len++;
            return len;
        }
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE: {
            ValueInterval interval = (ValueInterval) v;
            return 2 + getVarLongLen(interval.getLeading());
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
            return 2 + getVarLongLen(interval.getLeading()) + getVarLongLen(interval.getRemaining());
        }
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                byte[] b = v.getBytesNoCopy();
                return 1 + getVarIntLen(v.getValueType())
                    + getVarIntLen(b.length) + b.length;
            }
            throw DbException.throwInternalError("type=" + v.getValueType());
        }
    }

    /**
     * Set the current read / write position.
     *
     * @param pos the new position
     */
    public void setPos(int pos) {
        this.pos = pos;
    }

    /**
     * Write a short integer at the current position.
     * The current position is incremented.
     *
     * @param x the value
     */
    public void writeShortInt(int x) {
        byte[] buff = data;
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    /**
     * Read an short integer at the current position.
     * The current position is incremented.
     *
     * @return the value
     */
    public short readShortInt() {
        byte[] buff = data;
        return (short) (((buff[pos++] & 0xff) << 8) + (buff[pos++] & 0xff));
    }

    /**
     * Shrink the array to this size.
     *
     * @param size the new size
     */
    public void truncate(int size) {
        if (pos > size) {
            byte[] buff = Arrays.copyOf(data, size);
            this.pos = size;
            data = buff;
        }
    }

    /**
     * The number of bytes required for a variable size int.
     *
     * @param x the value
     * @return the len
     */
    private static int getVarIntLen(int x) {
        if ((x & (-1 << 7)) == 0) {
            return 1;
        } else if ((x & (-1 << 14)) == 0) {
            return 2;
        } else if ((x & (-1 << 21)) == 0) {
            return 3;
        } else if ((x & (-1 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    /**
     * Write a variable size int.
     *
     * @param x the value
     */
    public void writeVarInt(int x) {
        while ((x & ~0x7f) != 0) {
            data[pos++] = (byte) (x | 0x80);
            x >>>= 7;
        }
        data[pos++] = (byte) x;
    }

    /**
     * Read a variable size int.
     *
     * @return the value
     */
    public int readVarInt() {
        int b = data[pos];
        if (b >= 0) {
            pos++;
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(b);
    }

    private int readVarIntRest(int b) {
        int x = b & 0x7f;
        b = data[pos + 1];
        if (b >= 0) {
            pos += 2;
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = data[pos + 2];
        if (b >= 0) {
            pos += 3;
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = data[pos + 3];
        if (b >= 0) {
            pos += 4;
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (data[pos + 4] << 28);
        pos += 5;
        return x;
    }

    /**
     * The number of bytes required for a variable size long.
     *
     * @param x the value
     * @return the len
     */
    public static int getVarLongLen(long x) {
        int i = 1;
        while (true) {
            x >>>= 7;
            if (x == 0) {
                return i;
            }
            i++;
        }
    }

    /**
     * Write a variable size long.
     *
     * @param x the value
     */
    public void writeVarLong(long x) {
        while ((x & ~0x7f) != 0) {
            data[pos++] = (byte) (x | 0x80);
            x >>>= 7;
        }
        data[pos++] = (byte) x;
    }

    /**
     * Read a variable size long.
     *
     * @return the value
     */
    public long readVarLong() {
        long x = data[pos++];
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            long b = data[pos++];
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    /**
     * Check if there is still enough capacity in the buffer.
     * This method extends the buffer if required.
     *
     * @param plus the number of additional bytes required
     */
    public void checkCapacity(int plus) {
        if (pos + plus >= data.length) {
            // a separate method to simplify inlining
            expand(plus);
        }
    }

    private void expand(int plus) {
        // must copy everything, because pos could be 0 and data may be
        // still required
        data = Utils.copyBytes(data, (data.length + plus) * 2);
    }

    /**
     * Fill up the buffer with empty space and an (initially empty) checksum
     * until the size is a multiple of Constants.FILE_BLOCK_SIZE.
     */
    public void fillAligned() {
        // 0..6 > 8, 7..14 > 16, 15..22 > 24, ...
        int len = MathUtils.roundUpInt(pos + 2, Constants.FILE_BLOCK_SIZE);
        pos = len;
        if (data.length < len) {
            checkCapacity(len - data.length);
        }
    }

    /**
     * Copy a String from a reader to an output stream.
     *
     * @param source the reader
     * @param target the output stream
     */
    public static void copyString(Reader source, OutputStream target)
            throws IOException {
        char[] buff = new char[Constants.IO_BUFFER_SIZE];
        Data d = new Data(null, new byte[3 * Constants.IO_BUFFER_SIZE], false);
        while (true) {
            int l = source.read(buff);
            if (l < 0) {
                break;
            }
            d.writeStringWithoutLength(buff, l);
            target.write(d.data, 0, d.pos);
            d.reset();
        }
    }

    public DataHandler getHandler() {
        return handler;
    }

}
