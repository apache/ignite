/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.RangeInputStream;
import org.h2.store.RangeReader;
import org.h2.store.fs.FileUtils;
import org.h2.util.Bits;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * This is the legacy implementation of LOBs for PageStore databases where the
 * LOB was stored in an external file.
 */
public class ValueLob extends Value {

    private static final int BLOCK_COMPARISON_SIZE = 512;

    private static void rangeCheckUnknown(long zeroBasedOffset, long length) {
        if (zeroBasedOffset < 0) {
            throw DbException.getInvalidValueException("offset", zeroBasedOffset + 1);
        }
        if (length < 0) {
            throw DbException.getInvalidValueException("length", length);
        }
    }

    /**
     * Create an input stream that is s subset of the given stream.
     *
     * @param inputStream the source input stream
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the length of the result, in bytes
     * @param dataSize the length of the input, in bytes
     * @return the smaller input stream
     */
    static InputStream rangeInputStream(InputStream inputStream, long oneBasedOffset, long length, long dataSize) {
        if (dataSize > 0) {
            rangeCheck(oneBasedOffset - 1, length, dataSize);
        } else {
            rangeCheckUnknown(oneBasedOffset - 1, length);
        }
        try {
            return new RangeInputStream(inputStream, oneBasedOffset - 1, length);
        } catch (IOException e) {
            throw DbException.getInvalidValueException("offset", oneBasedOffset);
        }
    }

    /**
     * Create a reader that is s subset of the given reader.
     *
     * @param reader the input reader
     * @param oneBasedOffset the offset (1 means no offset)
     * @param length the length of the result, in bytes
     * @param dataSize the length of the input, in bytes
     * @return the smaller input stream
     */
    static Reader rangeReader(Reader reader, long oneBasedOffset, long length, long dataSize) {
        if (dataSize > 0) {
            rangeCheck(oneBasedOffset - 1, length, dataSize);
        } else {
            rangeCheckUnknown(oneBasedOffset - 1, length);
        }
        try {
            return new RangeReader(reader, oneBasedOffset - 1, length);
        } catch (IOException e) {
            throw DbException.getInvalidValueException("offset", oneBasedOffset);
        }
    }

    /**
     * Compares LOBs of the same type.
     *
     * @param v1 first LOB value
     * @param v2 second LOB value
     * @return result of comparison
     */
    static int compare(Value v1, Value v2) {
        int valueType = v1.getValueType();
        assert valueType == v2.getValueType();
        if (v1 instanceof ValueLobDb && v2 instanceof ValueLobDb) {
            byte[] small1 = v1.getSmall(), small2 = v2.getSmall();
            if (small1 != null && small2 != null) {
                if (valueType == Value.BLOB) {
                    return Bits.compareNotNullSigned(small1, small2);
                } else {
                    return Integer.signum(v1.getString().compareTo(v2.getString()));
                }
            }
        }
        long minPrec = Math.min(v1.getType().getPrecision(), v2.getType().getPrecision());
        if (valueType == Value.BLOB) {
            try (InputStream is1 = v1.getInputStream();
                    InputStream is2 = v2.getInputStream()) {
                byte[] buf1 = new byte[BLOCK_COMPARISON_SIZE];
                byte[] buf2 = new byte[BLOCK_COMPARISON_SIZE];
                for (; minPrec >= BLOCK_COMPARISON_SIZE; minPrec -= BLOCK_COMPARISON_SIZE) {
                    if (IOUtils.readFully(is1, buf1, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE
                            || IOUtils.readFully(is2, buf2, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE) {
                        throw DbException.getUnsupportedException("Invalid LOB");
                    }
                    int cmp = Bits.compareNotNullSigned(buf1, buf2);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                for (;;) {
                    int c1 = is1.read(), c2 = is2.read();
                    if (c1 < 0) {
                        return c2 < 0 ? 0 : -1;
                    }
                    if (c2 < 0) {
                        return 1;
                    }
                    if (c1 != c2) {
                        return Integer.compare(c1, c2);
                    }
                }
            } catch (IOException ex) {
                throw DbException.convert(ex);
            }
        } else {
            try (Reader reader1 = v1.getReader();
                    Reader reader2 = v2.getReader()) {
                char[] buf1 = new char[BLOCK_COMPARISON_SIZE];
                char[] buf2 = new char[BLOCK_COMPARISON_SIZE];
                for (; minPrec >= BLOCK_COMPARISON_SIZE; minPrec -= BLOCK_COMPARISON_SIZE) {
                    if (IOUtils.readFully(reader1, buf1, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE
                            || IOUtils.readFully(reader2, buf2, BLOCK_COMPARISON_SIZE) != BLOCK_COMPARISON_SIZE) {
                        throw DbException.getUnsupportedException("Invalid LOB");
                    }
                    int cmp = Bits.compareNotNull(buf1, buf2);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                for (;;) {
                    int c1 = reader1.read(), c2 = reader2.read();
                    if (c1 < 0) {
                        return c2 < 0 ? 0 : -1;
                    }
                    if (c2 < 0) {
                        return 1;
                    }
                    if (c1 != c2) {
                        return Integer.compare(c1, c2);
                    }
                }
            } catch (IOException ex) {
                throw DbException.convert(ex);
            }
        }
    }

    /**
     * This counter is used to calculate the next directory to store lobs. It is
     * better than using a random number because less directories are created.
     */
    private static int dirCounter;

    /**
     * either Value.BLOB or Value.CLOB
     */
    private final int valueType;
    private TypeInfo type;
    private final long precision;
    private final DataHandler handler;
    private int tableId;
    private final int objectId;
    private String fileName;
    private boolean linked;
    private int hash;
    private final boolean compressed;

    private ValueLob(int type, DataHandler handler, String fileName,
            int tableId, int objectId, boolean linked, long precision,
            boolean compressed) {
        this.valueType = type;
        this.handler = handler;
        this.fileName = fileName;
        this.tableId = tableId;
        this.objectId = objectId;
        this.linked = linked;
        this.precision = precision;
        this.compressed = compressed;
    }

    private static String getFileName(DataHandler handler, int tableId,
            int objectId) {
        if (tableId == 0 && objectId == 0) {
            DbException.throwInternalError("0 LOB");
        }
        String table = tableId < 0 ? ".temp" : ".t" + tableId;
        return getFileNamePrefix(handler.getDatabasePath(), objectId) +
                table + Constants.SUFFIX_LOB_FILE;
    }

    /**
     * Create a LOB value with the given parameters.
     *
     * @param type the data type, either Value.BLOB or Value.CLOB
     * @param handler the file handler
     * @param tableId the table object id
     * @param objectId the object id
     * @param precision the precision (length in elements)
     * @param compression if compression is used
     * @return the value object
     */
    public static ValueLob openLinked(int type, DataHandler handler,
            int tableId, int objectId, long precision, boolean compression) {
        String fileName = getFileName(handler, tableId, objectId);
        return new ValueLob(type, handler, fileName, tableId, objectId,
                true/* linked */, precision, compression);
    }

    /**
     * Create a LOB value with the given parameters.
     *
     * @param type the data type, either Value.BLOB or Value.CLOB
     * @param handler the file handler
     * @param tableId the table object id
     * @param objectId the object id
     * @param precision the precision (length in elements)
     * @param compression if compression is used
     * @param fileName the file name
     * @return the value object
     */
    public static ValueLob openUnlinked(int type, DataHandler handler,
            int tableId, int objectId, long precision, boolean compression,
            String fileName) {
        return new ValueLob(type, handler, fileName, tableId, objectId,
                false/* linked */, precision, compression);
    }

    private static String getFileNamePrefix(String path, int objectId) {
        String name;
        int f = objectId % SysProperties.LOB_FILES_PER_DIRECTORY;
        if (f > 0) {
            name = SysProperties.FILE_SEPARATOR + objectId;
        } else {
            name = "";
        }
        objectId /= SysProperties.LOB_FILES_PER_DIRECTORY;
        while (objectId > 0) {
            f = objectId % SysProperties.LOB_FILES_PER_DIRECTORY;
            name = SysProperties.FILE_SEPARATOR + f +
                    Constants.SUFFIX_LOBS_DIRECTORY + name;
            objectId /= SysProperties.LOB_FILES_PER_DIRECTORY;
        }
        name = FileUtils.toRealPath(path +
                Constants.SUFFIX_LOBS_DIRECTORY + name);
        return name;
    }

    private static int getNewObjectId(DataHandler h) {
        String path = h.getDatabasePath();
        if (path != null && path.isEmpty()) {
            path = new File(Utils.getProperty("java.io.tmpdir", "."),
                    SysProperties.PREFIX_TEMP_FILE).getAbsolutePath();
        }
        int newId = 0;
        int lobsPerDir = SysProperties.LOB_FILES_PER_DIRECTORY;
        while (true) {
            String dir = getFileNamePrefix(path, newId);
            String[] list = getFileList(h, dir);
            int fileCount = 0;
            boolean[] used = new boolean[lobsPerDir];
            for (String name : list) {
                if (name.endsWith(Constants.SUFFIX_DB_FILE)) {
                    name = FileUtils.getName(name);
                    String n = name.substring(0, name.indexOf('.'));
                    int id;
                    try {
                        id = Integer.parseInt(n);
                    } catch (NumberFormatException e) {
                        id = -1;
                    }
                    if (id > 0) {
                        fileCount++;
                        used[id % lobsPerDir] = true;
                    }
                }
            }
            int fileId = -1;
            if (fileCount < lobsPerDir) {
                for (int i = 1; i < lobsPerDir; i++) {
                    if (!used[i]) {
                        fileId = i;
                        break;
                    }
                }
            }
            if (fileId > 0) {
                newId += fileId;
                invalidateFileList(h, dir);
                break;
            }
            if (newId > Integer.MAX_VALUE / lobsPerDir) {
                // this directory path is full: start from zero
                newId = 0;
                dirCounter = MathUtils.randomInt(lobsPerDir - 1) * lobsPerDir;
            } else {
                // calculate the directory.
                // start with 1 (otherwise we don't know the number of
                // directories).
                // it doesn't really matter what directory is used, it might as
                // well be random (but that would generate more directories):
                // int dirId = RandomUtils.nextInt(lobsPerDir - 1) + 1;
                int dirId = (dirCounter++ / (lobsPerDir - 1)) + 1;
                newId = newId * lobsPerDir;
                newId += dirId * lobsPerDir;
            }
        }
        return newId;
    }

    private static void invalidateFileList(DataHandler h, String dir) {
        SmallLRUCache<String, String[]> cache = h.getLobFileListCache();
        if (cache != null) {
            synchronized (cache) {
                cache.remove(dir);
            }
        }
    }

    private static String[] getFileList(DataHandler h, String dir) {
        SmallLRUCache<String, String[]> cache = h.getLobFileListCache();
        String[] list;
        if (cache == null) {
            list = FileUtils.newDirectoryStream(dir).toArray(new String[0]);
        } else {
            synchronized (cache) {
                list = cache.get(dir);
                if (list == null) {
                    list = FileUtils.newDirectoryStream(dir).toArray(new String[0]);
                    cache.put(dir, list);
                }
            }
        }
        return list;
    }

    /**
     * Convert a lob to another data type. The data is fully read in memory
     * except when converting to BLOB or CLOB.
     *
     * @param t the new type
     * @param mode the database mode
     * @param column the column (if any), used for to improve the error message if conversion fails
     * @param extTypeInfo the extended data type information, or null
     * @return the converted value
     */
    @Override
    protected Value convertTo(int t, Mode mode, Object column, ExtTypeInfo extTypeInfo) {
        if (t == valueType) {
            return this;
        } else if (t == Value.CLOB) {
            return ValueLobDb.createTempClob(getReader(), -1, handler);
        } else if (t == Value.BLOB) {
            return ValueLobDb.createTempBlob(getInputStream(), -1, handler);
        }
        return super.convertTo(t, mode, column, null);
    }

    @Override
    public boolean isLinkedToTable() {
        return linked;
    }

    /**
     * Get the current file name where the lob is saved.
     *
     * @return the file name or null
     */
    public String getFileName() {
        return fileName;
    }

    @Override
    public void remove() {
        deleteFile(handler, fileName);
    }

    @Override
    public Value copy(DataHandler h, int tabId) {
        if (linked) {
            ValueLob copy = new ValueLob(this.valueType, this.handler, this.fileName,
                    this.tableId, getNewObjectId(h), this.linked, this.precision, this.compressed);
            copy.hash = this.hash;
            copy.tableId = tabId;
            String live = getFileName(h, copy.tableId, copy.objectId);
            copyFileTo(h, fileName, live);
            copy.fileName = live;
            copy.linked = true;
            return copy;
        }
        if (!linked) {
            this.tableId = tabId;
            String live = getFileName(h, tableId, objectId);
            renameFile(h, fileName, live);
            fileName = live;
            linked = true;
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

    /**
     * Get the current object id of this lob.
     *
     * @return the object id
     */
    public int getObjectId() {
        return objectId;
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
                return IOUtils.readStringAndClose(getReader(), len);
            }
            byte[] buff = IOUtils.readBytesAndClose(getInputStream(), len);
            return StringUtils.convertBytesToHex(buff);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    @Override
    public byte[] getBytes() {
        if (valueType == CLOB) {
            // convert hex to string
            return super.getBytes();
        }
        byte[] data = getBytesNoCopy();
        return Utils.cloneByteArray(data);
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (valueType == CLOB) {
            // convert hex to string
            return super.getBytesNoCopy();
        }
        try {
            return IOUtils.readBytesAndClose(
                    getInputStream(), Integer.MAX_VALUE);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
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
                hash = Utils.getByteArrayHash(getBytes());
            }
        }
        return hash;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        return compare(this, v);
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
        return rangeReader(getReader(), oneBasedOffset, length, valueType == Value.CLOB ? precision : -1);
    }

    @Override
    public InputStream getInputStream() {
        FileStore store = handler.openFile(fileName, "r", true);
        boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        return new BufferedInputStream(
                new FileStoreInputStream(store, handler, compressed, alwaysClose),
                Constants.IO_BUFFER_SIZE);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        FileStore store = handler.openFile(fileName, "r", true);
        boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        InputStream inputStream = new BufferedInputStream(
                new FileStoreInputStream(store, handler, compressed, alwaysClose),
                Constants.IO_BUFFER_SIZE);
        return rangeInputStream(inputStream, oneBasedOffset, length, store.length());
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
        StringBuilder buff = new StringBuilder();
        if (valueType == Value.CLOB) {
            buff.append("SPACE(").append(precision);
        } else {
            buff.append("CAST(REPEAT('00', ").append(precision).append(") AS BINARY");
        }
        buff.append(" /* ").append(fileName).append(" */)");
        return buff.toString();
    }

    /**
     * Get the data if this a small lob value.
     *
     * @return the data
     */
    @Override
    public byte[] getSmall() {
        return null;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ValueLob) {
            ValueLob o = (ValueLob) other;
            return valueType == o.valueType && compareTypeSafe(o, null) == 0;
        }
        return false;
    }

    /**
     * Check if this lob value is compressed.
     *
     * @return true if it is
     */
    public boolean isCompressed() {
        return compressed;
    }

    private static synchronized void deleteFile(DataHandler handler,
            String fileName) {
        // synchronize on the database, to avoid concurrent temp file creation /
        // deletion / backup
        synchronized (handler.getLobSyncObject()) {
            FileUtils.delete(fileName);
        }
    }

    private static synchronized void renameFile(DataHandler handler,
            String oldName, String newName) {
        synchronized (handler.getLobSyncObject()) {
            FileUtils.move(oldName, newName);
        }
    }

    private static void copyFileTo(DataHandler h, String sourceFileName,
            String targetFileName) {
        synchronized (h.getLobSyncObject()) {
            try {
                IOUtils.copyFiles(sourceFileName, targetFileName);
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
    }

    @Override
    public int getMemory() {
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
        ValueLobDb lob;
        if (valueType == CLOB) {
            lob = ValueLobDb.createTempClob(getReader(), precision, handler);
        } else {
            lob = ValueLobDb.createTempBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLobDb lob;
        if (valueType == CLOB) {
            lob = ValueLobDb.createTempClob(getReader(), precision, handler);
        } else {
            lob = ValueLobDb.createTempBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

}
