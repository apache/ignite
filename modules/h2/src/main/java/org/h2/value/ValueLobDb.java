/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;
import org.h2.store.RangeReader;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A implementation of the BLOB and CLOB data types.
 *
 * Small objects are kept in memory and stored in the record.
 * Large objects are either stored in the database, or in temporary files.
 */
public class ValueLobDb extends Value {

    /**
     * the value type (Value.BLOB or CLOB)
     */
    private final int valueType;

    private TypeInfo type;
    /**
     * If the LOB is managed by the one the LobStorageBackend classes, these are the
     * unique key inside that storage.
     */
    private final int tableId;
    private final long lobId;
    /**
     * If this is a client-side ValueLobDb object returned by a ResultSet, the
     * hmac acts a security cookie that the client can send back to the server
     * to ask for data related to this LOB.
     */
    private final byte[] hmac;
    /**
     * If the LOB is below the inline size, we just store/load it directly
     * here.
     */
    private final byte[] small;
    private final DataHandler handler;
    /**
     * For a BLOB, precision is length in bytes.
     * For a CLOB, precision is length in chars.
     */
    private final long precision;
    /**
     * If the LOB is a temporary LOB being managed by a temporary ResultSet,
     * it is stored in a temporary file.
     */
    private final String fileName;
    private final FileStore tempFile;
    /**
     * Cache the hashCode because it can be expensive to compute.
     */
    private int hash;

    //Arbonaut: 13.07.2016
    // Fix for recovery tool.

    private boolean isRecoveryReference;

    private ValueLobDb(int type, DataHandler handler, int tableId, long lobId,
            byte[] hmac, long precision) {
        this.valueType = type;
        this.handler = handler;
        this.tableId = tableId;
        this.lobId = lobId;
        this.hmac = hmac;
        this.precision = precision;
        this.small = null;
        this.fileName = null;
        this.tempFile = null;
    }

    private ValueLobDb(int type, byte[] small, long precision) {
        this.valueType = type;
        this.small = small;
        this.precision = precision;
        this.lobId = 0;
        this.hmac = null;
        this.handler = null;
        this.fileName = null;
        this.tempFile = null;
        this.tableId = 0;
    }

    /**
     * Create a CLOB in a temporary file.
     */
    private ValueLobDb(DataHandler handler, Reader in, long remaining)
            throws IOException {
        this.valueType = Value.CLOB;
        this.handler = handler;
        this.small = null;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = createTempLobFileName(handler);
        this.tempFile = this.handler.openFile(fileName, "rw", false);
        this.tempFile.autoDelete();

        long tmpPrecision = 0;
        try (FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null)) {
            char[] buff = new char[Constants.IO_BUFFER_SIZE];
            while (true) {
                int len = getBufferSize(this.handler, false, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len == 0) {
                    break;
                }
                byte[] data = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                out.write(data);
                tmpPrecision += len;
            }
        }
        this.precision = tmpPrecision;
        this.tableId = 0;
    }

    /**
     * Create a BLOB in a temporary file.
     */
    private ValueLobDb(DataHandler handler, byte[] buff, int len, InputStream in,
            long remaining) throws IOException {
        this.valueType = Value.BLOB;
        this.handler = handler;
        this.small = null;
        this.lobId = 0;
        this.hmac = null;
        this.fileName = createTempLobFileName(handler);
        this.tempFile = this.handler.openFile(fileName, "rw", false);
        this.tempFile.autoDelete();
        long tmpPrecision = 0;
        boolean compress = this.handler.getLobCompressionAlgorithm(Value.BLOB) != null;
        try (FileStoreOutputStream out = new FileStoreOutputStream(tempFile, null, null)) {
            while (true) {
                tmpPrecision += len;
                out.write(buff, 0, len);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(this.handler, compress, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len <= 0) {
                    break;
                }
            }
        }
        this.precision = tmpPrecision;
        this.tableId = 0;
    }

    private static String createTempLobFileName(DataHandler handler)
            throws IOException {
        String path = handler.getDatabasePath();
        if (path.isEmpty()) {
            path = SysProperties.PREFIX_TEMP_FILE;
        }
        return FileUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true);
    }

    /**
     * Create a LOB value.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param handler the data handler
     * @param tableId the table id
     * @param id the lob id
     * @param hmac the message authentication code
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLobDb create(int type, DataHandler handler,
            int tableId, long id, byte[] hmac, long precision) {
        return new ValueLobDb(type, handler, tableId, id, hmac, precision);
    }

    /**
     * Convert a lob to another data type. The data is fully read in memory
     * except when converting to BLOB or CLOB.
     *
     * @param t the new type
     * @param mode the mode
     * @param column the column (if any), used for to improve the error message if conversion fails
     * @param extTypeInfo the extended data type information, or null
     * @return the converted value
     */
    @Override
    protected Value convertTo(int t, Mode mode, Object column, ExtTypeInfo extTypeInfo) {
        if (t == valueType) {
            return this;
        } else if (t == Value.CLOB) {
            if (handler != null) {
                return handler.getLobStorage().
                        createClob(getReader(), -1);
            } else if (small != null) {
                return ValueLobDb.createSmallLob(t, small);
            }
        } else if (t == Value.BLOB) {
            if (handler != null) {
                return handler.getLobStorage().
                        createBlob(getInputStream(), -1);
            } else if (small != null) {
                return ValueLobDb.createSmallLob(t, small);
            }
        }
        return super.convertTo(t, mode, column, null);
    }

    @Override
    public boolean isLinkedToTable() {
        return small == null &&
                tableId >= 0;
    }

    public boolean isStored() {
        return small == null && fileName == null;
    }

    @Override
    public void remove() {
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
            }
            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup
            synchronized (handler.getLobSyncObject()) {
                FileUtils.delete(fileName);
            }
        }
        if (handler != null) {
            handler.getLobStorage().removeLob(this);
        }
    }

    @Override
    public Value copy(DataHandler database, int tableId) {
        if (small == null) {
            return handler.getLobStorage().copyLob(this, tableId, precision);
        } else if (small.length > database.getMaxLengthInplaceLob()) {
            LobStorageInterface s = database.getLobStorage();
            Value v;
            if (valueType == Value.BLOB) {
                v = s.createBlob(getInputStream(), precision);
            } else {
                v = s.createClob(getReader(), precision);
            }
            Value v2 = v.copy(database, tableId);
            v.remove();
            return v2;
        }
        return this;
    }

    /**
     * Get the current table id of this lob.
     *
     * @return the table id
     */
    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = new TypeInfo(valueType, precision, 0, MathUtils.convertLongToInt(precision), null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return valueType;
    }

    @Override
    public String getString() {
        int len = precision > Integer.MAX_VALUE || precision == 0 ?
                Integer.MAX_VALUE : (int) precision;
        try {
            if (valueType == Value.CLOB) {
                if (small != null) {
                    return new String(small, StandardCharsets.UTF_8);
                }
                return IOUtils.readStringAndClose(getReader(), len);
            }
            byte[] buff;
            if (small != null) {
                buff = small;
            } else {
                buff = IOUtils.readBytesAndClose(getInputStream(), len);
            }
            return StringUtils.convertBytesToHex(buff);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public byte[] getBytes() {
        if (valueType == CLOB) {
            // convert hex to string
            return super.getBytes();
        }
        if (small != null) {
            return Utils.cloneByteArray(small);
        }
        try {
            return IOUtils.readBytesAndClose(getInputStream(), Integer.MAX_VALUE);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (valueType == CLOB) {
            // convert hex to string
            return super.getBytesNoCopy();
        }
        if (small != null) {
            return small;
        }
        try {
            return IOUtils.readBytesAndClose(getInputStream(), Integer.MAX_VALUE);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            if (precision > 4096) {
                // TODO: should calculate the hash code when saving, and store
                // it in the database file
                return (int) (precision ^ (precision >>> 32));
            }
            if (valueType == CLOB) {
                hash = getString().hashCode();
            } else {
                if (small != null) {
                    hash = Utils.getByteArrayHash(small);
                } else {
                    hash = Utils.getByteArrayHash(getBytes());
                }
            }
        }
        return hash;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        if (v instanceof ValueLobDb) {
            ValueLobDb v2 = (ValueLobDb) v;
            if (v == this) {
                return 0;
            }
            if (lobId == v2.lobId && small == null && v2.small == null) {
                return 0;
            }
        }
        return ValueLob.compare(this, v);
    }

    @Override
    public Object getObject() {
        if (valueType == Value.CLOB) {
            return getReader();
        }
        return getInputStream();
    }

    @Override
    public Reader getReader() {
        return IOUtils.getBufferedReader(getInputStream());
    }

    @Override
    public Reader getReader(long oneBasedOffset, long length) {
        return ValueLob.rangeReader(getReader(), oneBasedOffset, length, valueType == Value.CLOB ? precision : -1);
    }

    @Override
    public InputStream getInputStream() {
        if (small != null) {
            return new ByteArrayInputStream(small);
        } else if (fileName != null) {
            FileStore store = handler.openFile(fileName, "r", true);
            boolean alwaysClose = SysProperties.lobCloseBetweenReads;
            return new BufferedInputStream(new FileStoreInputStream(store,
                    handler, false, alwaysClose), Constants.IO_BUFFER_SIZE);
        }
        long byteCount = (valueType == Value.BLOB) ? precision : -1;
        try {
            return handler.getLobStorage().getInputStream(this, hmac, byteCount);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        long byteCount;
        InputStream inputStream;
        if (small != null) {
            return super.getInputStream(oneBasedOffset, length);
        } else if (fileName != null) {
            FileStore store = handler.openFile(fileName, "r", true);
            boolean alwaysClose = SysProperties.lobCloseBetweenReads;
            byteCount = store.length();
            inputStream = new BufferedInputStream(new FileStoreInputStream(store,
                    handler, false, alwaysClose), Constants.IO_BUFFER_SIZE);
        } else {
            byteCount = (valueType == Value.BLOB) ? precision : -1;
            try {
                inputStream = handler.getLobStorage().getInputStream(this, hmac, byteCount);
            } catch (IOException e) {
                throw DbException.convertIOException(e, toString());
            }
        }
        return ValueLob.rangeInputStream(inputStream, oneBasedOffset, length, byteCount);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        long p = precision;
        if (p > Integer.MAX_VALUE || p <= 0) {
            p = -1;
        }
        if (valueType == Value.BLOB) {
            prep.setBinaryStream(parameterIndex, getInputStream(), (int) p);
        } else {
            prep.setCharacterStream(parameterIndex, getReader(), (int) p);
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        if (valueType == Value.CLOB) {
            StringUtils.quoteStringSQL(builder, getString());
        } else {
            builder.append("X'");
            StringUtils.convertBytesToHex(builder, getBytes()).append('\'');
        }
        return builder;
    }

    @Override
    public String getTraceSQL() {
        if (small != null && precision <= SysProperties.MAX_TRACE_DATA_LENGTH) {
            return getSQL();
        }
        StringBuilder buff = new StringBuilder();
        if (valueType == Value.CLOB) {
            buff.append("SPACE(").append(precision);
        } else {
            buff.append("CAST(REPEAT('00', ").append(precision).append(") AS BINARY");
        }
        buff.append(" /* table: ").append(tableId).append(" id: ")
                .append(lobId).append(" */)");
        return buff.toString();
    }

    /**
     * Get the data if this a small lob value.
     *
     * @return the data
     */
    @Override
    public byte[] getSmall() {
        return small;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueLobDb))
            return false;
        ValueLobDb otherLob = (ValueLobDb) other;
        if (hashCode() != otherLob.hashCode())
            return false;
        return compareTypeSafe((Value) other, null) == 0;
    }

    @Override
    public int getMemory() {
        if (small != null) {
            /*
             * Java 11 with -XX:-UseCompressedOops
             * 0 bytes: 120 bytes
             * 1 byte: 128 bytes
             */
            return small.length + 127;
        }
        return 140;
    }

    /**
     * Create an independent copy of this temporary value.
     * The file will not be deleted automatically.
     *
     * @return the value
     */
    @Override
    public ValueLobDb copyToTemp() {
        return this;
    }

    /**
     * Create an independent copy of this value,
     * that will be bound to a result.
     *
     * @return the value (this for small objects)
     */
    @Override
    public ValueLobDb copyToResult() {
        if (handler == null) {
            return this;
        }
        LobStorageInterface s = handler.getLobStorage();
        if (s.isReadOnly()) {
            return this;
        }
        return s.copyLob(this, LobStorageFrontend.TABLE_RESULT, precision);
    }

    public long getLobId() {
        return lobId;
    }

    @Override
    public String toString() {
        return "lob: " + fileName + " table: " + tableId + " id: " + lobId;
    }

    /**
     * Create a temporary CLOB value from a stream.
     *
     * @param in the reader
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLobDb createTempClob(Reader in, long length,
            DataHandler handler) {
        if (length >= 0) {
            // Otherwise BufferedReader may try to read more data than needed and that
            // blocks the network level
            try {
                in = new RangeReader(in, 0, length);
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
        BufferedReader reader;
        if (in instanceof BufferedReader) {
            reader = (BufferedReader) in;
        } else {
            reader = new BufferedReader(in, Constants.IO_BUFFER_SIZE);
        }
        try {
            boolean compress = handler.getLobCompressionAlgorithm(Value.CLOB) != null;
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(handler, compress, remaining);
            char[] buff;
            if (len >= Integer.MAX_VALUE) {
                String data = IOUtils.readStringAndClose(reader, -1);
                buff = data.toCharArray();
                len = buff.length;
            } else {
                buff = new char[len];
                reader.mark(len);
                len = IOUtils.readFully(reader, buff, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                return ValueLobDb.createSmallLob(Value.CLOB, small, len);
            }
            reader.reset();
            return new ValueLobDb(handler, reader, remaining);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * Create a temporary BLOB value from a stream.
     *
     * @param in the input stream
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLobDb createTempBlob(InputStream in, long length,
            DataHandler handler) {
        try {
            long remaining = Long.MAX_VALUE;
            boolean compress = handler.getLobCompressionAlgorithm(Value.BLOB) != null;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(handler, compress, remaining);
            byte[] buff;
            if (len >= Integer.MAX_VALUE) {
                buff = IOUtils.readBytesAndClose(in, -1);
                len = buff.length;
            } else {
                buff = Utils.newBytes(len);
                len = IOUtils.readFully(in, buff, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = Utils.copyBytes(buff, len);
                return ValueLobDb.createSmallLob(Value.BLOB, small, small.length);
            }
            return new ValueLobDb(handler, buff, len, in, remaining);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private static int getBufferSize(DataHandler handler, boolean compress,
            long remaining) {
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        int inplace = handler.getMaxLengthInplaceLob();
        long m = compress ? Constants.IO_BUFFER_SIZE_COMPRESS
                : Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            // using "1L" to force long arithmetic because
            // inplace could be Integer.MAX_VALUE
            m = Math.min(remaining, inplace + 1L);
            // the buffer size must be bigger than the inplace lob, otherwise we
            // can't know if it must be stored in-place or not
            m = MathUtils.roundUpLong(m, Constants.IO_BUFFER_SIZE);
        }
        m = Math.min(remaining, m);
        m = MathUtils.convertLongToInt(m);
        if (m < 0) {
            m = Integer.MAX_VALUE;
        }
        return (int) m;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLobDb lob;
        if (valueType == CLOB) {
            if (handler == null) {
                try {
                    int p = MathUtils.convertLongToInt(precision);
                    String s = IOUtils.readStringAndClose(getReader(), p);
                    byte[] data = s.getBytes(StandardCharsets.UTF_8);
                    lob = ValueLobDb.createSmallLob(valueType, data, s.length());
                } catch (IOException e) {
                    throw DbException.convertIOException(e, null);
                }
            } else {
                lob = ValueLobDb.createTempClob(getReader(), precision, handler);
            }
        } else {
            if (handler == null) {
                try {
                    int p = MathUtils.convertLongToInt(precision);
                    byte[] data = IOUtils.readBytesAndClose(getInputStream(), p);
                    lob = ValueLobDb.createSmallLob(valueType, data, data.length);
                } catch (IOException e) {
                    throw DbException.convertIOException(e, null);
                }
            } else {
                lob = ValueLobDb.createTempBlob(getInputStream(), precision, handler);
            }
        }
        return lob;
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the LOB
     */
    public static ValueLobDb createSmallLob(int type, byte[] small) {
        int precision;
        if (type == Value.CLOB) {
            precision = new String(small, StandardCharsets.UTF_8).length();
        } else {
            precision = small.length;
        }
        return createSmallLob(type, small, precision);
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @param precision the precision
     * @return the LOB
     */
    public static ValueLobDb createSmallLob(int type, byte[] small,
            long precision) {
        return new ValueLobDb(type, small, precision);
    }


    public void setRecoveryReference(boolean isRecoveryReference) {
        this.isRecoveryReference = isRecoveryReference;
    }

    public boolean isRecoveryReference() {
        return isRecoveryReference;
    }
}
