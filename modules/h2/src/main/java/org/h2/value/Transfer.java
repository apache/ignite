/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataReader;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
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
        return in.readByte() == 1;
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
            int len = s.length();
            out.writeInt(len);
            for (int i = 0; i < len; i++) {
                out.writeChar(s.charAt(i));
            }
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
        int type = v.getType();
        writeInt(type);
        switch (type) {
        case Value.NULL:
            break;
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            writeBoolean(v.getBoolean());
            break;
        case Value.BYTE:
            writeByte(v.getByte());
            break;
        case Value.TIME:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                writeLong(((ValueTime) v).getNanos());
            } else {
                writeLong(DateTimeUtils.getTimeLocalWithoutDst(v.getTime()));
            }
            break;
        case Value.DATE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                writeLong(((ValueDate) v).getDateValue());
            } else {
                writeLong(DateTimeUtils.getTimeLocalWithoutDst(v.getDate()));
            }
            break;
        case Value.TIMESTAMP: {
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                ValueTimestamp ts = (ValueTimestamp) v;
                writeLong(ts.getDateValue());
                writeLong(ts.getTimeNanos());
            } else {
                Timestamp ts = v.getTimestamp();
                writeLong(DateTimeUtils.getTimeLocalWithoutDst(ts));
                writeInt(ts.getNanos() % 1_000_000);
            }
            break;
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getTimeNanos());
            writeInt(ts.getTimeZoneOffsetMins());
            break;
        }
        case Value.DECIMAL:
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeDouble(v.getDouble());
            break;
        case Value.FLOAT:
            writeFloat(v.getFloat());
            break;
        case Value.INT:
            writeInt(v.getInt());
            break;
        case Value.LONG:
            writeLong(v.getLong());
            break;
        case Value.SHORT:
            writeInt(v.getShort());
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            writeString(v.getString());
            break;
        case Value.BLOB: {
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
                        writeLong(lob.getPrecision());
                        break;
                    }
                }
            }
            long length = v.getPrecision();
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
                        writeLong(lob.getPrecision());
                        break;
                    }
                }
            }
            long length = v.getPrecision();
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
        case Value.ENUM: {
            writeInt(v.getInt());
            writeString(v.getString());
            break;
        }
        case Value.RESULT_SET: {
            try {
                ResultSet rs = ((ValueResultSet) v).getResultSet();
                rs.beforeFirst();
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                writeInt(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    writeString(meta.getColumnName(i + 1));
                    writeInt(meta.getColumnType(i + 1));
                    writeInt(meta.getPrecision(i + 1));
                    writeInt(meta.getScale(i + 1));
                }
                while (rs.next()) {
                    writeBoolean(true);
                    for (int i = 0; i < columnCount; i++) {
                        int t = DataType.getValueTypeFromResultSet(meta, i + 1);
                        Value val = DataType.readValue(session, rs, i + 1, t);
                        writeValue(val);
                    }
                }
                writeBoolean(false);
                rs.beforeFirst();
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
            break;
        }
        case Value.GEOMETRY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_14) {
                writeBytes(v.getBytesNoCopy());
            } else {
                writeString(v.getString());
            }
            break;
        default:
            if (JdbcUtils.customDataTypesHandler != null) {
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
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BYTES:
            return ValueBytes.getNoCopy(readBytes());
        case Value.UUID:
            return ValueUuid.get(readLong(), readLong());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(null, readBytes(), session.getDataHandler());
        case Value.BOOLEAN:
            return ValueBoolean.get(readBoolean());
        case Value.BYTE:
            return ValueByte.get(readByte());
        case Value.DATE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                return ValueDate.fromDateValue(readLong());
            } else {
                return ValueDate.fromMillis(DateTimeUtils.getTimeUTCWithoutDst(readLong()));
            }
        case Value.TIME:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                return ValueTime.fromNanos(readLong());
            } else {
                return ValueTime.fromMillis(DateTimeUtils.getTimeUTCWithoutDst(readLong()));
            }
        case Value.TIMESTAMP: {
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                return ValueTimestamp.fromDateValueAndNanos(
                        readLong(), readLong());
            } else {
                return ValueTimestamp.fromMillisNanos(
                        DateTimeUtils.getTimeUTCWithoutDst(readLong()),
                        readInt() % 1_000_000);
            }
        }
        case Value.TIMESTAMP_TZ: {
            return ValueTimestampTimeZone.fromDateValueAndNanos(readLong(),
                    readLong(), (short) readInt());
        }
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(readString()));
        case Value.DOUBLE:
            return ValueDouble.get(readDouble());
        case Value.FLOAT:
            return ValueFloat.get(readFloat());
        case Value.ENUM: {
            final int ordinal = readInt();
            final String label = readString();
            return ValueEnumBase.get(label, ordinal);
        }
        case Value.INT:
            return ValueInt.get(readInt());
        case Value.LONG:
            return ValueLong.get(readLong());
        case Value.SHORT:
            return ValueShort.get((short) readInt());
        case Value.STRING:
            return ValueString.get(readString());
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(readString());
        case Value.STRING_FIXED:
            return ValueStringFixed.get(readString(), ValueStringFixed.PRECISION_DO_NOT_TRIM, null);
        case Value.BLOB: {
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
        case Value.CLOB: {
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
        case Value.ARRAY: {
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
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            rs.setAutoClose(false);
            int columns = readInt();
            for (int i = 0; i < columns; i++) {
                rs.addColumn(readString(), readInt(), readInt(), readInt());
            }
            while (readBoolean()) {
                Object[] o = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue().getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case Value.GEOMETRY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_14) {
                return ValueGeometry.get(readBytes());
            }
            return ValueGeometry.get(readString());
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
     * Open a new new connection to the same address and port as this one.
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
