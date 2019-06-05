/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
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
 * Implementation of the BLOB and CLOB data types. Small objects are kept in
 * memory and stored in the record.
 *
 * Large objects are stored in their own files. When large objects are set in a
 * prepared statement, they are first stored as 'temporary' files. Later, when
 * they are used in a record, and when the record is stored, the lob files are
 * linked: the file is renamed using the file format (tableId).(objectId). There
 * is one exception: large variables are stored in the file (-1).(objectId).
 *
 * When lobs are deleted, they are first renamed to a temp file, and if the
 * delete operation is committed the file is deleted.
 *
 * Data compression is supported.
 */
public class ValueLob extends Value {

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
     * This counter is used to calculate the next directory to store lobs. It is
     * better than using a random number because less directories are created.
     */
    private static int dirCounter;

    private final int type;
    private long precision;
    private DataHandler handler;
    private int tableId;
    private int objectId;
    private String fileName;
    private boolean linked;
    private byte[] small;
    private int hash;
    private boolean compressed;
    private FileStore tempFile;

    private ValueLob(int type, DataHandler handler, String fileName,
            int tableId, int objectId, boolean linked, long precision,
            boolean compressed) {
        this.type = type;
        this.handler = handler;
        this.fileName = fileName;
        this.tableId = tableId;
        this.objectId = objectId;
        this.linked = linked;
        this.precision = precision;
        this.compressed = compressed;
    }

    private ValueLob(int type, byte[] small) {
        this.type = type;
        this.small = small;
        if (small != null) {
            if (type == Value.BLOB) {
                this.precision = small.length;
            } else {
                this.precision = getString().length();
            }
        }
    }

    private static ValueLob copy(ValueLob lob) {
        ValueLob copy = new ValueLob(lob.type, lob.handler, lob.fileName,
                lob.tableId, lob.objectId, lob.linked, lob.precision, lob.compressed);
        copy.small = lob.small;
        copy.hash = lob.hash;
        return copy;
    }

    /**
     * Create a small lob using the given byte array.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the lob value
     */
    private static ValueLob createSmallLob(int type, byte[] small) {
        return new ValueLob(type, small);
    }

    private static String getFileName(DataHandler handler, int tableId,
            int objectId) {
        if (SysProperties.CHECK && tableId == 0 && objectId == 0) {
            DbException.throwInternalError("0 LOB");
        }
        String table = tableId < 0 ? ".temp" : ".t" + tableId;
        return getFileNamePrefix(handler.getDatabasePath(), objectId) +
                table + Constants.SUFFIX_LOB_FILE;
    }

    /**
     * Create a LOB value with the given parameters.
     *
     * @param type the data type
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
     * @param type the data type
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

    /**
     * Create a CLOB value from a stream.
     *
     * @param in the reader
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    private static ValueLob createClob(Reader in, long length,
            DataHandler handler) {
        try {
            if (handler == null) {
                String s = IOUtils.readStringAndClose(in, (int) length);
                return createSmallLob(Value.CLOB, s.getBytes(StandardCharsets.UTF_8));
            }
            boolean compress = handler.getLobCompressionAlgorithm(Value.CLOB) != null;
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(handler, compress, remaining);
            char[] buff;
            if (len >= Integer.MAX_VALUE) {
                String data = IOUtils.readStringAndClose(in, -1);
                buff = data.toCharArray();
                len = buff.length;
            } else {
                buff = new char[len];
                len = IOUtils.readFully(in, buff, len);
            }
            if (len <= handler.getMaxLengthInplaceLob()) {
                byte[] small = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                return ValueLob.createSmallLob(Value.CLOB, small);
            }
            ValueLob lob = new ValueLob(Value.CLOB, null);
            lob.createFromReader(buff, len, in, remaining, handler);
            return lob;
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
        long m = compress ?
                Constants.IO_BUFFER_SIZE_COMPRESS : Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            // using "1L" to force long arithmetic
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

    private void createFromReader(char[] buff, int len, Reader in,
            long remaining, DataHandler h) throws IOException {
        try (FileStoreOutputStream out = initLarge(h)) {
            boolean compress = h.getLobCompressionAlgorithm(Value.CLOB) != null;
            while (true) {
                precision += len;
                byte[] b = new String(buff, 0, len).getBytes(StandardCharsets.UTF_8);
                out.write(b, 0, b.length);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(h, compress, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len == 0) {
                    break;
                }
            }
        }
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
        if ((path != null) && (path.length() == 0)) {
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
     * Create a BLOB value from a stream.
     *
     * @param in the input stream
     * @param length the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    private static ValueLob createBlob(InputStream in, long length,
            DataHandler handler) {
        try {
            if (handler == null) {
                byte[] data = IOUtils.readBytesAndClose(in, (int) length);
                return createSmallLob(Value.BLOB, data);
            }
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
                return ValueLob.createSmallLob(Value.BLOB, small);
            }
            ValueLob lob = new ValueLob(Value.BLOB, null);
            lob.createFromStream(buff, len, in, remaining, handler);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private FileStoreOutputStream initLarge(DataHandler h) {
        this.handler = h;
        this.tableId = 0;
        this.linked = false;
        this.precision = 0;
        this.small = null;
        this.hash = 0;
        String compressionAlgorithm = h.getLobCompressionAlgorithm(type);
        this.compressed = compressionAlgorithm != null;
        synchronized (h) {
            String path = h.getDatabasePath();
            if ((path != null) && (path.length() == 0)) {
                path = new File(Utils.getProperty("java.io.tmpdir", "."),
                        SysProperties.PREFIX_TEMP_FILE).getAbsolutePath();
            }
            objectId = getNewObjectId(h);
            fileName = getFileNamePrefix(path, objectId) + Constants.SUFFIX_TEMP_FILE;
            tempFile = h.openFile(fileName, "rw", false);
            tempFile.autoDelete();
        }
        return new FileStoreOutputStream(tempFile, h,
                compressionAlgorithm);
    }

    private void createFromStream(byte[] buff, int len, InputStream in,
            long remaining, DataHandler h) throws IOException {
        try (FileStoreOutputStream out = initLarge(h)) {
            boolean compress = h.getLobCompressionAlgorithm(Value.BLOB) != null;
            while (true) {
                precision += len;
                out.write(buff, 0, len);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(h, compress, remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len <= 0) {
                    break;
                }
            }
        }
    }

    /**
     * Convert a lob to another data type. The data is fully read in memory
     * except when converting to BLOB or CLOB.
     *
     * @param t the new type
     * @param precision the precision of the column to convert this value to.
     *        The special constant <code>-1</code> is used to indicate that
     *        the precision plays no role when converting the value
     * @param mode the database mode
     * @param column the column (if any), used for to improve the error message if conversion fails
     * @param enumerators the ENUM datatype enumerators (if any),
     *        for dealing with ENUM conversions
     * @return the converted value
     */
    @Override
    public Value convertTo(int t, int precision, Mode mode, Object column, String[] enumerators) {
        if (t == type) {
            return this;
        } else if (t == Value.CLOB) {
            return ValueLob.createClob(getReader(), -1, handler);
        } else if (t == Value.BLOB) {
            return ValueLob.createBlob(getInputStream(), -1, handler);
        }
        return super.convertTo(t, precision, mode, column, null);
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
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
                tempFile = null;
            }
            deleteFile(handler, fileName);
        }
    }

    @Override
    public Value copy(DataHandler h, int tabId) {
        if (fileName == null) {
            this.tableId = tabId;
            return this;
        }
        if (linked) {
            ValueLob copy = ValueLob.copy(this);
            copy.objectId = getNewObjectId(h);
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
            if (tempFile != null) {
                tempFile.stopAutoDelete();
                tempFile = null;
            }
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
    public int getType() {
        return type;
    }

    @Override
    public long getPrecision() {
        return precision;
    }

    @Override
    public String getString() {
        int len = precision > Integer.MAX_VALUE || precision == 0 ?
                Integer.MAX_VALUE : (int) precision;
        try {
            if (type == Value.CLOB) {
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
            throw DbException.convertIOException(e, fileName);
        }
    }

    @Override
    public byte[] getBytes() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytes();
        }
        byte[] data = getBytesNoCopy();
        return Utils.cloneByteArray(data);
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytesNoCopy();
        }
        if (small != null) {
            return small;
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
            if (type == CLOB) {
                hash = getString().hashCode();
            } else {
                hash = Utils.getByteArrayHash(getBytes());
            }
        }
        return hash;
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        if (type == Value.CLOB) {
            return Integer.signum(getString().compareTo(v.getString()));
        }
        byte[] v2 = v.getBytesNoCopy();
        return Bits.compareNotNullSigned(getBytesNoCopy(), v2);
    }

    @Override
    public Object getObject() {
        if (type == Value.CLOB) {
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
        return rangeReader(getReader(), oneBasedOffset, length, type == Value.CLOB ? precision : -1);
    }

    @Override
    public InputStream getInputStream() {
        if (fileName == null) {
            return new ByteArrayInputStream(small);
        }
        FileStore store = handler.openFile(fileName, "r", true);
        boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        return new BufferedInputStream(
                new FileStoreInputStream(store, handler, compressed, alwaysClose),
                Constants.IO_BUFFER_SIZE);
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        if (fileName == null) {
            return super.getInputStream(oneBasedOffset, length);
        }
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
        long p = getPrecision();
        if (p > Integer.MAX_VALUE || p <= 0) {
            p = -1;
        }
        if (type == Value.BLOB) {
            prep.setBinaryStream(parameterIndex, getInputStream(), (int) p);
        } else {
            prep.setCharacterStream(parameterIndex, getReader(), (int) p);
        }
    }

    @Override
    public String getSQL() {
        String s;
        if (type == Value.CLOB) {
            s = getString();
            return StringUtils.quoteStringSQL(s);
        }
        byte[] buff = getBytes();
        s = StringUtils.convertBytesToHex(buff);
        return "X'" + s + "'";
    }

    @Override
    public String getTraceSQL() {
        if (small != null && getPrecision() <= SysProperties.MAX_TRACE_DATA_LENGTH) {
            return getSQL();
        }
        StringBuilder buff = new StringBuilder();
        if (type == Value.CLOB) {
            buff.append("SPACE(").append(getPrecision());
        } else {
            buff.append("CAST(REPEAT('00', ").append(getPrecision()).append(") AS BINARY");
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
        return small;
    }

    @Override
    public int getDisplaySize() {
        return MathUtils.convertLongToInt(getPrecision());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueLob && compareSecure((Value) other, null) == 0;
    }

    /**
     * Store the lob data to a file if the size of the buffer is larger than the
     * maximum size for an in-place lob.
     *
     * @param h the data handler
     */
    public void convertToFileIfRequired(DataHandler h) {
        try {
            if (small != null && small.length > h.getMaxLengthInplaceLob()) {
                boolean compress = h.getLobCompressionAlgorithm(type) != null;
                int len = getBufferSize(h, compress, Long.MAX_VALUE);
                int tabId = tableId;
                if (type == Value.BLOB) {
                    createFromStream(
                            Utils.newBytes(len), 0, getInputStream(), Long.MAX_VALUE, h);
                } else {
                    createFromReader(
                            new char[len], 0, getReader(), Long.MAX_VALUE, h);
                }
                Value v2 = copy(h, tabId);
                if (SysProperties.CHECK && v2 != this) {
                    DbException.throwInternalError(v2.toString());
                }
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
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
        if (small != null) {
            return small.length + 104;
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
    public ValueLob copyToTemp() {
        ValueLob lob;
        if (type == CLOB) {
            lob = ValueLob.createClob(getReader(), precision, handler);
        } else {
            lob = ValueLob.createBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLob lob;
        if (type == CLOB) {
            lob = ValueLob.createClob(getReader(), precision, handler);
        } else {
            lob = ValueLob.createBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

}
