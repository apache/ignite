/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.Socket;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataReader;
import org.h2.util.Bits;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * The transfer class is used to send and receive Value objects.
 * It is used on both the client side, and on the server side.
 */
public class Transfer {

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int LOB_MAGIC = 0x1234;
    private static final int LOB_MAC_SALT_LENGTH = 16;

    private static final int NULL = 0;
    private static final int BOOLEAN = 1;
    private static final int BYTE = 2;
    private static final int SHORT = 3;
    private static final int INT = 4;
    private static final int LONG = 5;
    private static final int DECIMAL = 6;
    private static final int DOUBLE = 7;
    private static final int FLOAT = 8;
    private static final int TIME = 9;
    private static final int DATE = 10;
    private static final int TIMESTAMP = 11;
    private static final int BYTES = 12;
    private static final int STRING = 13;
    private static final int STRING_IGNORECASE = 14;
    private static final int BLOB = 15;
    private static final int CLOB = 16;
    private static final int ARRAY = 17;
    private static final int RESULT_SET = 18;
    private static final int JAVA_OBJECT = 19;
    private static final int UUID = 20;
    private static final int STRING_FIXED = 21;
    private static final int GEOMETRY = 22;
    private static final int TIMESTAMP_TZ = 24;
    private static final int ENUM = 25;
    private static final int INTERVAL = 26;
    private static final int ROW = 27;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private SessionInterface session;
    private boolean ssl;
    private int version;
    private byte[] lobMacSalt;

    /**
     * Create a new transfer object for the specified session.
     *
     * @param session the session
     * @param s the socket
     */
    public Transfer(SessionInterface session, Socket s) {
        this.session = session;
        this.socket = s;
    }

    /**
     * Initialize the transfer object. This method will try to open an input and
     * output stream.
     */
    public synchronized void init() throws IOException {
        if (socket != null) {
            in = new DataInputStream(
                    new BufferedInputStream(
                            socket.getInputStream(), Transfer.BUFFER_SIZE));
            out = new DataOutputStream(
                    new BufferedOutputStream(
                            socket.getOutputStream(), Transfer.BUFFER_SIZE));
        }
    }

    /**
     * Write pending changes.
     */
    public void flush() throws IOException {
        out.flush();
    }

    /**
     * Write a boolean.
     *
     * @param x the value
     * @return itself
     */
    public Transfer writeBoolean(boolean x) throws IOException {
        out.writeByte((byte) (x ? 1 : 0));
        return this;
    }

    /**
     * Read a boolean.
     *
     * @return the value
     */
    public boolean readBoolean() throws IOException {
        return in.readByte() != 0;
    }

    /**
     * Write a byte.
     *
     * @param x the value
     * @return itself
     */
    private Transfer writeByte(byte x) throws IOException {
        out.writeByte(x);
        return this;
    }

    /**
     * Read a byte.
     *
     * @return the value
     */
    private byte readByte() throws IOException {
        return in.readByte();
    }

    /**
     * Write an int.
     *
     * @param x the value
     * @return itself
     */
    public Transfer writeInt(int x) throws IOException {
        out.writeInt(x);
        return this;
    }

    /**
     * Read an int.
     *
     * @return the value
     */
    public int readInt() throws IOException {
        return in.readInt();
    }

    /**
     * Write a long.
     *
     * @param x the value
     * @return itself
     */
    public Transfer writeLong(long x) throws IOException {
        out.writeLong(x);
        return this;
    }

    /**
     * Read a long.
     *
     * @return the value
     */
    public long readLong() throws IOException {
        return in.readLong();
    }

    /**
     * Write a double.
     *
     * @param i the value
     * @return itself
     */
    private Transfer writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    /**
     * Write a float.
     *
     * @param i the value
     * @return itself
     */
    private Transfer writeFloat(float i) throws IOException {
        out.writeFloat(i);
        return this;
    }

    /**
     * Read a double.
     *
     * @return the value
     */
    private double readDouble() throws IOException {
        return in.readDouble();
    }

    /**
     * Read a float.
     *
     * @return the value
     */
    private float readFloat() throws IOException {
        return in.readFloat();
    }

    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param s the value
     * @return itself
     */
    public Transfer writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(s.length());
            out.writeChars(s);
        }
        return this;
    }

    /**
     * Read a string.
     *
     * @return the value
     */
    public String readString() throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(in.readChar());
        }
        String s = buff.toString();
        s = StringUtils.cache(s);
        return s;
    }

    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
     */
    public Transfer writeBytes(byte[] data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
            out.write(data);
        }
        return this;
    }

    /**
     * Write a number of bytes.
     *
     * @param buff the value
     * @param off the offset
     * @param len the length
     * @return itself
     */
    public Transfer writeBytes(byte[] buff, int off, int len) throws IOException {
        out.write(buff, off, len);
        return this;
    }

    /**
     * Read a byte array.
     *
     * @return the value
     */
    public byte[] readBytes() throws IOException {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        byte[] b = Utils.newBytes(len);
        in.readFully(b);
        return b;
    }

    /**
     * Read a number of bytes.
     *
     * @param buff the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    public void readBytes(byte[] buff, int off, int len) throws IOException {
        in.readFully(buff, off, len);
    }

    /**
     * Close the transfer object and the socket.
     */
    public synchronized void close() {
        if (socket != null) {
            try {
                if (out != null) {
                    out.flush();
                }
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
                DbException.traceThrowable(e);
            } finally {
                socket = null;
            }
        }
    }

    /**
     * Write a value.
     *
     * @param v the value
     */
    public void writeValue(Value v) throws IOException {
        int type = v.getValueType();
        switch (type) {
        case Value.NULL:
            writeInt(NULL);
            break;
        case Value.BYTES:
            writeInt(BYTES);
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.JAVA_OBJECT:
            writeInt(JAVA_OBJECT);
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            writeInt(UUID);
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            writeInt(BOOLEAN);
            writeBoolean(v.getBoolean());
            break;
        case Value.BYTE:
            writeInt(BYTE);
            writeByte(v.getByte());
            break;
        case Value.TIME:
            writeInt(TIME);
            writeLong(((ValueTime) v).getNanos());
            break;
        case Value.DATE:
            writeInt(DATE);
            writeLong(((ValueDate) v).getDateValue());
            break;
        case Value.TIMESTAMP: {
            writeInt(TIMESTAMP);
            ValueTimestamp ts = (ValueTimestamp) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getTimeNanos());
            break;
        }
        case Value.TIMESTAMP_TZ: {
            writeInt(TIMESTAMP_TZ);
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getTimeNanos());
            writeInt(ts.getTimeZoneOffsetMins());
            break;
        }
        case Value.DECIMAL:
            writeInt(DECIMAL);
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeInt(DOUBLE);
            writeDouble(v.getDouble());
            break;
        case Value.FLOAT:
            writeInt(FLOAT);
            writeFloat(v.getFloat());
            break;
        case Value.INT:
            writeInt(INT);
            writeInt(v.getInt());
            break;
        case Value.LONG:
            writeInt(LONG);
            writeLong(v.getLong());
            break;
        case Value.SHORT:
            writeInt(SHORT);
            writeInt(v.getShort());
            break;
        case Value.STRING:
            writeInt(STRING);
            writeString(v.getString());
            break;
        case Value.STRING_IGNORECASE:
            writeInt(STRING_IGNORECASE);
            writeString(v.getString());
            break;
        case Value.STRING_FIXED:
            writeInt(STRING_FIXED);
            writeString(v.getString());
            break;
        case Value.BLOB: {
            writeInt(BLOB);
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (v instanceof ValueLobDb) {
                    ValueLobDb lob = (ValueLobDb) v;
                    if (lob.isStored()) {
                        writeLong(-1);
                        writeInt(lob.getTableId());
                        writeLong(lob.getLobId());
                        if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                            writeBytes(calculateLobMac(lob.getLobId()));
                        }
                        writeLong(lob.getType().getPrecision());
                        break;
                    }
                }
            }
            long length = v.getType().getPrecision();
            if (length < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            writeLong(length);
            long written = IOUtils.copyAndCloseInput(v.getInputStream(), out);
            if (written != length) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length:" + length + " written:" + written);
            }
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.CLOB: {
            writeInt(CLOB);
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (v instanceof ValueLobDb) {
                    ValueLobDb lob = (ValueLobDb) v;
                    if (lob.isStored()) {
                        writeLong(-1);
                        writeInt(lob.getTableId());
                        writeLong(lob.getLobId());
                        if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                            writeBytes(calculateLobMac(lob.getLobId()));
                        }
                        writeLong(lob.getType().getPrecision());
                        break;
                    }
                }
            }
            long length = v.getType().getPrecision();
            if (length < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            writeLong(length);
            Reader reader = v.getReader();
            Data.copyString(reader, out);
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.ARRAY: {
            writeInt(ARRAY);
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            Class<?> componentType = va.getComponentType();
            if (componentType == Object.class) {
                writeInt(len);
            } else {
                writeInt(-(len + 1));
                writeString(componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.ROW: {
            writeInt(version >= Constants.TCP_PROTOCOL_VERSION_18 ? ROW : ARRAY);
            ValueRow va = (ValueRow) v;
            Value[] list = va.getList();
            int len = list.length;
            writeInt(len);
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.ENUM: {
            writeInt(ENUM);
            writeInt(v.getInt());
            writeString(v.getString());
            break;
        }
        case Value.RESULT_SET: {
            writeInt(RESULT_SET);
            ResultInterface result = ((ValueResultSet) v).getResult();
            int columnCount = result.getVisibleColumnCount();
            writeInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                TypeInfo columnType = result.getColumnType(i);
                if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                    writeString(result.getAlias(i));
                    writeString(result.getColumnName(i));
                    writeInt(columnType.getValueType());
                    writeLong(columnType.getPrecision());
                } else {
                    writeString(result.getColumnName(i));
                    writeInt(DataType.getDataType(columnType.getValueType()).sqlType);
                    writeInt(MathUtils.convertLongToInt(columnType.getPrecision()));
                }
                writeInt(columnType.getScale());
            }
            while (result.next()) {
                writeBoolean(true);
                Value[] row = result.currentRow();
                for (int i = 0; i < columnCount; i++) {
                    writeValue(row[i]);
                }
            }
            writeBoolean(false);
            break;
        }
        case Value.GEOMETRY:
            writeInt(GEOMETRY);
            if (version >= Constants.TCP_PROTOCOL_VERSION_14) {
                writeBytes(v.getBytesNoCopy());
            } else {
                writeString(v.getString());
            }
            break;
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                ValueInterval interval = (ValueInterval) v;
                int ordinal = type - Value.INTERVAL_YEAR;
                if (interval.isNegative()) {
                    ordinal = ~ordinal;
                }
                writeInt(INTERVAL);
                writeByte((byte) ordinal);
                writeLong(interval.getLeading());
            } else {
                writeInt(STRING);
                writeString(v.getString());
            }
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                ValueInterval interval = (ValueInterval) v;
                int ordinal = type - Value.INTERVAL_YEAR;
                if (interval.isNegative()) {
                    ordinal = ~ordinal;
                }
                writeInt(INTERVAL);
                writeByte((byte) ordinal);
                writeLong(interval.getLeading());
                writeLong(interval.getRemaining());
            } else {
                writeInt(STRING);
                writeString(v.getString());
            }
            break;
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                writeInt(type);
                writeBytes(v.getBytesNoCopy());
                break;
            }
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public Value readValue() throws IOException {
        int type = readInt();
        switch (type) {
        case NULL:
            return ValueNull.INSTANCE;
        case BYTES:
            return ValueBytes.getNoCopy(readBytes());
        case UUID:
            return ValueUuid.get(readLong(), readLong());
        case JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(null, readBytes(), session.getDataHandler());
        case BOOLEAN:
            return ValueBoolean.get(readBoolean());
        case BYTE:
            return ValueByte.get(readByte());
        case DATE:
            return ValueDate.fromDateValue(readLong());
        case TIME:
            return ValueTime.fromNanos(readLong());
        case TIMESTAMP:
            return ValueTimestamp.fromDateValueAndNanos(readLong(), readLong());
        case TIMESTAMP_TZ: {
            return ValueTimestampTimeZone.fromDateValueAndNanos(readLong(), readLong(), (short) readInt());
        }
        case DECIMAL:
            return ValueDecimal.get(new BigDecimal(readString()));
        case DOUBLE:
            return ValueDouble.get(readDouble());
        case FLOAT:
            return ValueFloat.get(readFloat());
        case ENUM: {
            final int ordinal = readInt();
            final String label = readString();
            return ValueEnumBase.get(label, ordinal);
        }
        case INT:
            return ValueInt.get(readInt());
        case LONG:
            return ValueLong.get(readLong());
        case SHORT:
            return ValueShort.get((short) readInt());
        case STRING:
            return ValueString.get(readString());
        case STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case STRING_FIXED:
            return ValueStringFixed.get(readString());
        case BLOB: {
            long length = readLong();
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (length == -1) {
                    int tableId = readInt();
                    long id = readLong();
                    byte[] hmac;
                    if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                        hmac = readBytes();
                    } else {
                        hmac = null;
                    }
                    long precision = readLong();
                    return ValueLobDb.create(
                            Value.BLOB, session.getDataHandler(), tableId, id, hmac, precision);
                }
            }
            Value v = session.getDataHandler().getLobStorage().createBlob(in, length);
            int magic = readInt();
            if (magic != LOB_MAGIC) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;
        }
        case CLOB: {
            long length = readLong();
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (length == -1) {
                    int tableId = readInt();
                    long id = readLong();
                    byte[] hmac;
                    if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                        hmac = readBytes();
                    } else {
                        hmac = null;
                    }
                    long precision = readLong();
                    return ValueLobDb.create(
                            Value.CLOB, session.getDataHandler(), tableId, id, hmac, precision);
                }
                if (length < 0) {
                    throw DbException.get(
                            ErrorCode.CONNECTION_BROKEN_1, "length="+ length);
                }
            }
            Value v = session.getDataHandler().getLobStorage().
                    createClob(new DataReader(in), length);
            int magic = readInt();
            if (magic != LOB_MAGIC) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;
        }
        case ARRAY: {
            int len = readInt();
            Class<?> componentType = Object.class;
            if (len < 0) {
                len = -(len + 1);
                componentType = JdbcUtils.loadUserClass(readString());
            }
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return ValueArray.get(componentType, list);
        }
        case ROW: {
            int len = readInt();
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue();
            }
            return ValueRow.get(list);
        }
        case RESULT_SET: {
            SimpleResult rs = new SimpleResult();
            int columns = readInt();
            for (int i = 0; i < columns; i++) {
                if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                    rs.addColumn(readString(), readString(), readInt(), readLong(), readInt());
                } else {
                    String name = readString();
                    rs.addColumn(name, name, DataType.convertSQLTypeToValueType(readInt()), readInt(), readInt());
                }
            }
            while (readBoolean()) {
                Value[] o = new Value[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case GEOMETRY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_14) {
                return ValueGeometry.get(readBytes());
            }
            return ValueGeometry.get(readString());
        case INTERVAL: {
            int ordinal = readByte();
            boolean negative = ordinal < 0;
            if (negative) {
                ordinal = ~ordinal;
            }
            return ValueInterval.from(IntervalQualifier.valueOf(ordinal), negative, readLong(),
                    ordinal < 5 ? 0 : readLong());
        }
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
                return JdbcUtils.customDataTypesHandler.convert(
                        ValueBytes.getNoCopy(readBytes()), type);
            }
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * Get the socket.
     *
     * @return the socket
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * Set the session.
     *
     * @param session the session
     */
    public void setSession(SessionInterface session) {
        this.session = session;
    }

    /**
     * Enable or disable SSL.
     *
     * @param ssl the new value
     */
    public void setSSL(boolean ssl) {
        this.ssl = ssl;
    }

    /**
     * Open a new connection to the same address and port as this one.
     *
     * @return the new transfer object
     */
    public Transfer openNewConnection() throws IOException {
        InetAddress address = socket.getInetAddress();
        int port = socket.getPort();
        Socket s2 = NetUtils.createSocket(address, port, ssl);
        Transfer trans = new Transfer(null, s2);
        trans.setSSL(ssl);
        return trans;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public synchronized boolean isClosed() {
        return socket == null || socket.isClosed();
    }

    /**
     * Verify the HMAC.
     *
     * @param hmac the message authentication code
     * @param lobId the lobId
     * @throws DbException if the HMAC does not match
     */
    public void verifyLobMac(byte[] hmac, long lobId) {
        byte[] result = calculateLobMac(lobId);
        if (!Utils.compareSecure(hmac,  result)) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "Invalid lob hmac; possibly the connection was re-opened internally");
        }
    }

    private byte[] calculateLobMac(long lobId) {
        if (lobMacSalt == null) {
            lobMacSalt = MathUtils.secureRandomBytes(LOB_MAC_SALT_LENGTH);
        }
        byte[] data = new byte[8];
        Bits.writeLong(data, 0, lobId);
        return SHA256.getHashWithSalt(data, lobMacSalt);
    }

}
