/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.SysProperties;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.tools.CompressTool;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;

/**
 * This class stores LOB objects in the database, in tables. This is the
 * back-end i.e. the server side of the LOB storage.
 * <p>
 * Using the system session
 * <p>
 * Why do we use the system session to store the data? Some LOB operations can
 * take a very long time. If we did them on a normal session, we would be
 * locking the LOB tables for long periods of time, which is extremely
 * detrimental to the rest of the system. Perhaps when we shift to the MVStore
 * engine, we can revisit this design decision (using the StreamStore, that is,
 * no connection at all).
 * <p>
 * Locking
 * <p>
 * Normally, the locking order in H2 is: first lock the Session object, then
 * lock the Database object. However, in the case of the LOB data, we are using
 * the system session to store the data. If we locked the normal way, we see
 * deadlocks caused by the following pattern:
 *
 * <pre>
 *  Thread 1:
 *     locks normal session
 *     locks database
 *     waiting to lock system session
 *  Thread 2:
 *      locks system session
 *      waiting to lock database.
 * </pre>
 *
 * So, in this class alone, we do two things: we have our very own dedicated
 * session, the LOB session, and we take the locks in this order: first the
 * Database object, and then the LOB session. Since we own the LOB session,
 * no-one else can lock on it, and we are safe.
 */
public class LobStorageBackend implements LobStorageInterface {

    /**
     * The name of the lob data table. If this table exists, then lob storage is
     * used.
     */
    public static final String LOB_DATA_TABLE = "LOB_DATA";

    private static final String LOB_SCHEMA = "INFORMATION_SCHEMA";
    private static final String LOBS = LOB_SCHEMA + ".LOBS";
    private static final String LOB_MAP = LOB_SCHEMA + ".LOB_MAP";
    private static final String LOB_DATA = LOB_SCHEMA + "." + LOB_DATA_TABLE;

    /**
     * The size of the chunks we use when storing LOBs inside the database file.
     */
    private static final int BLOCK_LENGTH = 20_000;

    /**
     * The size of cache for lob block hashes. Each entry needs 2 longs (16
     * bytes), therefore, the size 4096 means 64 KB.
     */
    private static final int HASH_CACHE_SIZE = 4 * 1024;

    JdbcConnection conn;
    final Database database;

    private final HashMap<String, PreparedStatement> prepared = new HashMap<>();
    private long nextBlock;
    private final CompressTool compress = CompressTool.getInstance();
    private long[] hashBlocks;

    private boolean init;

    public LobStorageBackend(Database database) {
        this.database = database;
    }

    @Override
    public void init() {
        if (init) {
            return;
        }
        synchronized (database) {
            // have to check this again or we might miss an update on another
            // thread
            if (init) {
                return;
            }
            init = true;
            conn = database.getLobConnectionForRegularUse();
            JdbcConnection initConn = database.getLobConnectionForInit();
            try {
                Statement stat = initConn.createStatement();
                // stat.execute("SET UNDO_LOG 0");
                // stat.execute("SET REDO_LOG_BINARY 0");
                boolean create = true;
                PreparedStatement prep = initConn.prepareStatement(
                        "SELECT ZERO() FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
                        "TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?");
                prep.setString(1, "INFORMATION_SCHEMA");
                prep.setString(2, "LOB_MAP");
                prep.setString(3, "POS");
                ResultSet rs;
                rs = prep.executeQuery();
                if (rs.next()) {
                    prep = initConn.prepareStatement(
                            "SELECT ZERO() FROM INFORMATION_SCHEMA.TABLES WHERE " +
                            "TABLE_SCHEMA=? AND TABLE_NAME=?");
                    prep.setString(1, "INFORMATION_SCHEMA");
                    prep.setString(2, "LOB_DATA");
                    rs = prep.executeQuery();
                    if (rs.next()) {
                        create = false;
                    }
                }
                if (create) {
                    stat.execute("CREATE CACHED TABLE IF NOT EXISTS " + LOBS +
                            "(ID BIGINT PRIMARY KEY, BYTE_COUNT BIGINT, TABLE INT) HIDDEN");
                    stat.execute("CREATE INDEX IF NOT EXISTS " +
                            "INFORMATION_SCHEMA.INDEX_LOB_TABLE ON " +
                            LOBS + "(TABLE)");
                    stat.execute("CREATE CACHED TABLE IF NOT EXISTS " + LOB_MAP +
                            "(LOB BIGINT, SEQ INT, POS BIGINT, HASH INT, " +
                            "BLOCK BIGINT, PRIMARY KEY(LOB, SEQ)) HIDDEN");
                    stat.execute("ALTER TABLE " + LOB_MAP +
                            " RENAME TO " + LOB_MAP + " HIDDEN");
                    stat.execute("ALTER TABLE " + LOB_MAP +
                            " ADD IF NOT EXISTS POS BIGINT BEFORE HASH");
                    // TODO the column name OFFSET was used in version 1.3.156,
                    // so this can be remove in a later version
                    stat.execute("ALTER TABLE " + LOB_MAP +
                            " DROP COLUMN IF EXISTS \"OFFSET\"");
                    stat.execute("CREATE INDEX IF NOT EXISTS " +
                            "INFORMATION_SCHEMA.INDEX_LOB_MAP_DATA_LOB ON " +
                            LOB_MAP + "(BLOCK, LOB)");
                    stat.execute("CREATE CACHED TABLE IF NOT EXISTS " +
                            LOB_DATA +
                            "(BLOCK BIGINT PRIMARY KEY, COMPRESSED INT, DATA BINARY) HIDDEN");
                }
                rs = stat.executeQuery("SELECT MAX(BLOCK) FROM " + LOB_DATA);
                rs.next();
                nextBlock = rs.getLong(1) + 1;
                stat.close();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }

    private long getNextLobId() throws SQLException {
        String sql = "SELECT MAX(LOB) FROM " + LOB_MAP;
        PreparedStatement prep = prepare(sql);
        ResultSet rs = prep.executeQuery();
        rs.next();
        long x = rs.getLong(1) + 1;
        reuse(sql, prep);
        sql = "SELECT MAX(ID) FROM " + LOBS;
        prep = prepare(sql);
        rs = prep.executeQuery();
        rs.next();
        x = Math.max(x, rs.getLong(1) + 1);
        reuse(sql, prep);
        return x;
    }

    @Override
    public void removeAllForTable(int tableId) {
        init();
        try {
            String sql = "SELECT ID FROM " + LOBS + " WHERE TABLE = ?";
            PreparedStatement prep = prepare(sql);
            prep.setInt(1, tableId);
            ResultSet rs = prep.executeQuery();
            while (rs.next()) {
                removeLob(rs.getLong(1));
            }
            reuse(sql, prep);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        if (tableId == LobStorageFrontend.TABLE_ID_SESSION_VARIABLE) {
            removeAllForTable(LobStorageFrontend.TABLE_TEMP);
            removeAllForTable(LobStorageFrontend.TABLE_RESULT);
        }
    }

    /**
     * Read a block of data from the given LOB.
     *
     * @param block the block number
     * @return the block (expanded if stored compressed)
     */
    byte[] readBlock(long block) throws SQLException {
        // see locking discussion at the top
        assertNotHolds(conn.getSession());
        synchronized (database) {
            synchronized (conn.getSession()) {
                String sql = "SELECT COMPRESSED, DATA FROM " +
                        LOB_DATA + " WHERE BLOCK = ?";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, block);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1,
                            "Missing lob entry, block: " + block)
                            .getSQLException();
                }
                int compressed = rs.getInt(1);
                byte[] buffer = rs.getBytes(2);
                if (compressed != 0) {
                    buffer = compress.expand(buffer);
                }
                reuse(sql, prep);
                return buffer;
            }
        }
    }

    /**
     * Create a prepared statement, or re-use an existing one.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    PreparedStatement prepare(String sql) throws SQLException {
        if (SysProperties.CHECK2) {
            if (!Thread.holdsLock(database)) {
                throw DbException.throwInternalError();
            }
        }
        PreparedStatement prep = prepared.remove(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
        }
        return prep;
    }

    /**
     * Allow to re-use the prepared statement.
     *
     * @param sql the SQL statement
     * @param prep the prepared statement
     */
    void reuse(String sql, PreparedStatement prep) {
        if (SysProperties.CHECK2) {
            if (!Thread.holdsLock(database)) {
                throw DbException.throwInternalError();
            }
        }
        prepared.put(sql, prep);
    }

    @Override
    public void removeLob(ValueLobDb lob) {
        removeLob(lob.getLobId());
    }

    private void removeLob(long lobId) {
        try {
            // see locking discussion at the top
            assertNotHolds(conn.getSession());
            synchronized (database) {
                synchronized (conn.getSession()) {
                    String sql = "SELECT BLOCK, HASH FROM " + LOB_MAP + " D WHERE D.LOB = ? " +
                            "AND NOT EXISTS(SELECT 1 FROM " + LOB_MAP + " O " +
                            "WHERE O.BLOCK = D.BLOCK AND O.LOB <> ?)";
                    PreparedStatement prep = prepare(sql);
                    prep.setLong(1, lobId);
                    prep.setLong(2, lobId);
                    ResultSet rs = prep.executeQuery();
                    ArrayList<Long> blocks = New.arrayList();
                    while (rs.next()) {
                        blocks.add(rs.getLong(1));
                        int hash = rs.getInt(2);
                        setHashCacheBlock(hash, -1);
                    }
                    reuse(sql, prep);

                    sql = "DELETE FROM " + LOB_MAP + " WHERE LOB = ?";
                    prep = prepare(sql);
                    prep.setLong(1, lobId);
                    prep.execute();
                    reuse(sql, prep);

                    sql = "DELETE FROM " + LOB_DATA + " WHERE BLOCK = ?";
                    prep = prepare(sql);
                    for (long block : blocks) {
                        prep.setLong(1, block);
                        prep.execute();
                    }
                    reuse(sql, prep);

                    sql = "DELETE FROM " + LOBS + " WHERE ID = ?";
                    prep = prepare(sql);
                    prep.setLong(1, lobId);
                    prep.execute();
                    reuse(sql, prep);
                }
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public InputStream getInputStream(ValueLobDb lob, byte[] hmac,
            long byteCount) throws IOException {
        try {
            init();
            assertNotHolds(conn.getSession());
            // see locking discussion at the top
            synchronized (database) {
                synchronized (conn.getSession()) {
                    long lobId = lob.getLobId();
                    return new LobInputStream(lobId, byteCount);
                }
            }
        } catch (SQLException e) {
            throw DbException.convertToIOException(e);
        }
    }

    private ValueLobDb addLob(InputStream in, long maxLength, int type,
            CountingReaderInputStream countingReaderForClob) {
        try {
            byte[] buff = new byte[BLOCK_LENGTH];
            if (maxLength < 0) {
                maxLength = Long.MAX_VALUE;
            }
            long length = 0;
            long lobId = -1;
            int maxLengthInPlaceLob = database.getMaxLengthInplaceLob();
            String compressAlgorithm = database.getLobCompressionAlgorithm(type);
            try {
                byte[] small = null;
                for (int seq = 0; maxLength > 0; seq++) {
                    int len = (int) Math.min(BLOCK_LENGTH, maxLength);
                    len = IOUtils.readFully(in, buff, len);
                    if (len <= 0) {
                        break;
                    }
                    maxLength -= len;
                    // if we had a short read, trim the buffer
                    byte[] b;
                    if (len != buff.length) {
                        b = Arrays.copyOf(buff, len);
                    } else {
                        b = buff;
                    }
                    if (seq == 0 && b.length < BLOCK_LENGTH &&
                            b.length <= maxLengthInPlaceLob) {
                        small = b;
                        break;
                    }
                    assertNotHolds(conn.getSession());
                    // see locking discussion at the top
                    synchronized (database) {
                        synchronized (conn.getSession()) {
                            if (seq == 0) {
                                lobId = getNextLobId();
                            }
                            storeBlock(lobId, seq, length, b, compressAlgorithm);
                        }
                    }
                    length += len;
                }
                if (lobId == -1 && small == null) {
                    // zero length
                    small = new byte[0];
                }
                if (small != null) {
                    // For a BLOB, precision is length in bytes.
                    // For a CLOB, precision is length in chars
                    long precision = countingReaderForClob == null ?
                            small.length : countingReaderForClob.getLength();
                    return ValueLobDb.createSmallLob(type, small, precision);
                }
                // For a BLOB, precision is length in bytes.
                // For a CLOB, precision is length in chars
                long precision = countingReaderForClob == null ?
                        length : countingReaderForClob.getLength();
                return registerLob(type, lobId,
                        LobStorageFrontend.TABLE_TEMP, length, precision);
            } catch (IOException e) {
                if (lobId != -1) {
                    removeLob(lobId);
                }
                throw DbException.convertIOException(e, null);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private ValueLobDb registerLob(int type, long lobId, int tableId,
            long byteCount, long precision) throws SQLException {
        assertNotHolds(conn.getSession());
        // see locking discussion at the top
        synchronized (database) {
            synchronized (conn.getSession()) {
                String sql = "INSERT INTO " + LOBS +
                        "(ID, BYTE_COUNT, TABLE) VALUES(?, ?, ?)";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, lobId);
                prep.setLong(2, byteCount);
                prep.setInt(3, tableId);
                prep.execute();
                reuse(sql, prep);
                return ValueLobDb.create(type,
                        database, tableId, lobId, null, precision);
            }
        }
    }

    @Override
    public boolean isReadOnly() {
        return database.isReadOnly();
    }

    @Override
    public ValueLobDb copyLob(ValueLobDb old, int tableId, long length) {
        int type = old.getType();
        long oldLobId = old.getLobId();
        assertNotHolds(conn.getSession());
        // see locking discussion at the top
        synchronized (database) {
            synchronized (conn.getSession()) {
                try {
                    init();
                    ValueLobDb v = null;
                    if (!old.isRecoveryReference()) {
                        long lobId = getNextLobId();
                        String sql = "INSERT INTO " + LOB_MAP +
                                "(LOB, SEQ, POS, HASH, BLOCK) " +
                                "SELECT ?, SEQ, POS, HASH, BLOCK FROM " +
                                LOB_MAP + " WHERE LOB = ?";
                        PreparedStatement prep = prepare(sql);
                        prep.setLong(1, lobId);
                        prep.setLong(2, oldLobId);
                        prep.executeUpdate();
                        reuse(sql, prep);

                        sql = "INSERT INTO " + LOBS +
                                "(ID, BYTE_COUNT, TABLE) " +
                                "SELECT ?, BYTE_COUNT, ? FROM " + LOBS +
                                " WHERE ID = ?";
                        prep = prepare(sql);
                        prep.setLong(1, lobId);
                        prep.setLong(2, tableId);
                        prep.setLong(3, oldLobId);
                        prep.executeUpdate();
                        reuse(sql, prep);

                        v = ValueLobDb.create(type, database, tableId, lobId, null, length);
                    } else {
                        // Recovery process, no need to copy LOB using normal
                        // infrastructure
                        v = ValueLobDb.create(type, database, tableId, oldLobId, null, length);
                    }
                    return v;
                } catch (SQLException e) {
                    throw DbException.convert(e);
                }
            }
        }
    }

    private long getHashCacheBlock(int hash) {
        if (HASH_CACHE_SIZE > 0) {
            initHashCache();
            int index = hash & (HASH_CACHE_SIZE - 1);
            long oldHash = hashBlocks[index];
            if (oldHash == hash) {
                return hashBlocks[index + HASH_CACHE_SIZE];
            }
        }
        return -1;
    }

    private void setHashCacheBlock(int hash, long block) {
        if (HASH_CACHE_SIZE > 0) {
            initHashCache();
            int index = hash & (HASH_CACHE_SIZE - 1);
            hashBlocks[index] = hash;
            hashBlocks[index + HASH_CACHE_SIZE] = block;
        }
    }

    private void initHashCache() {
        if (hashBlocks == null) {
            hashBlocks = new long[HASH_CACHE_SIZE * 2];
        }
    }

    /**
     * Store a block in the LOB storage.
     *
     * @param lobId the lob id
     * @param seq the sequence number
     * @param pos the position within the lob
     * @param b the data
     * @param compressAlgorithm the compression algorithm (may be null)
     */
    void storeBlock(long lobId, int seq, long pos, byte[] b,
            String compressAlgorithm) throws SQLException {
        long block;
        boolean blockExists = false;
        if (compressAlgorithm != null) {
            b = compress.compress(b, compressAlgorithm);
        }
        int hash = Arrays.hashCode(b);
        assertHoldsLock(conn.getSession());
        assertHoldsLock(database);
        block = getHashCacheBlock(hash);
        if (block != -1) {
            String sql =  "SELECT COMPRESSED, DATA FROM " + LOB_DATA +
                    " WHERE BLOCK = ?";
            PreparedStatement prep = prepare(sql);
            prep.setLong(1, block);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                boolean compressed = rs.getInt(1) != 0;
                byte[] compare = rs.getBytes(2);
                if (compressed == (compressAlgorithm != null) && Arrays.equals(b, compare)) {
                    blockExists = true;
                }
            }
            reuse(sql, prep);
        }
        if (!blockExists) {
            block = nextBlock++;
            setHashCacheBlock(hash, block);
            String sql = "INSERT INTO " + LOB_DATA +
                    "(BLOCK, COMPRESSED, DATA) VALUES(?, ?, ?)";
            PreparedStatement prep = prepare(sql);
            prep.setLong(1, block);
            prep.setInt(2, compressAlgorithm == null ? 0 : 1);
            prep.setBytes(3, b);
            prep.execute();
            reuse(sql, prep);
        }
        String sql = "INSERT INTO " + LOB_MAP +
                "(LOB, SEQ, POS, HASH, BLOCK) VALUES(?, ?, ?, ?, ?)";
        PreparedStatement prep = prepare(sql);
        prep.setLong(1, lobId);
        prep.setInt(2, seq);
        prep.setLong(3, pos);
        prep.setLong(4, hash);
        prep.setLong(5, block);
        prep.execute();
        reuse(sql, prep);
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        init();
        return addLob(in, maxLength, Value.BLOB, null);
    }

    @Override
    public Value createClob(Reader reader, long maxLength) {
        init();
        long max = maxLength == -1 ? Long.MAX_VALUE : maxLength;
        CountingReaderInputStream in = new CountingReaderInputStream(reader, max);
        return addLob(in, Long.MAX_VALUE, Value.CLOB, in);
    }

    private static void assertNotHolds(Object lock) {
        if (Thread.holdsLock(lock)) {
            throw DbException.throwInternalError(lock.toString());
        }
    }

    /**
     * Check whether this thread has synchronized on this object.
     *
     * @param lock the object
     */
    static void assertHoldsLock(Object lock) {
        if (!Thread.holdsLock(lock)) {
            throw DbException.throwInternalError(lock.toString());
        }
    }

    /**
     * An input stream that reads from a LOB.
     */
    public class LobInputStream extends InputStream {

        /**
         * Data from the LOB_MAP table. We cache this to prevent other updates
         * to the table that contains the LOB column from changing the data
         * under us.
         */
        private final long[] lobMapBlocks;

        /**
         * index into the lobMapBlocks array.
         */
        private int lobMapIndex;

        /**
         * The remaining bytes in the lob.
         */
        private long remainingBytes;

        /**
         * The temporary buffer.
         */
        private byte[] buffer;

        /**
         * The position within the buffer.
         */
        private int bufferPos;


        public LobInputStream(long lobId, long byteCount) throws SQLException {

            // we have to take the lock on the session
            // before the lock on the database to prevent ABBA deadlocks
            assertHoldsLock(conn.getSession());
            assertHoldsLock(database);

            if (byteCount == -1) {
                String sql = "SELECT BYTE_COUNT FROM " + LOBS + " WHERE ID = ?";
                PreparedStatement prep = prepare(sql);
                prep.setLong(1, lobId);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1,
                            "Missing lob entry: " + lobId).getSQLException();
                }
                byteCount = rs.getLong(1);
                reuse(sql, prep);
            }
            this.remainingBytes = byteCount;

            String sql = "SELECT COUNT(*) FROM " + LOB_MAP + " WHERE LOB = ?";
            PreparedStatement prep = prepare(sql);
            prep.setLong(1, lobId);
            ResultSet rs = prep.executeQuery();
            rs.next();
            int lobMapCount = rs.getInt(1);
            if (lobMapCount == 0) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1,
                        "Missing lob entry: " + lobId).getSQLException();
            }
            reuse(sql, prep);

            this.lobMapBlocks = new long[lobMapCount];

            sql = "SELECT BLOCK FROM " + LOB_MAP + " WHERE LOB = ? ORDER BY SEQ";
            prep = prepare(sql);
            prep.setLong(1, lobId);
            rs = prep.executeQuery();
            int i = 0;
            while (rs.next()) {
                this.lobMapBlocks[i] = rs.getLong(1);
                i++;
            }
            reuse(sql, prep);
        }

        @Override
        public int read() throws IOException {
            fillBuffer();
            if (remainingBytes <= 0) {
                return -1;
            }
            remainingBytes--;
            return buffer[bufferPos++] & 255;
        }

        @Override
        public long skip(long n) throws IOException {
            if (n <= 0) {
                return 0;
            }
            long remaining = n;
            remaining -= skipSmall(remaining);
            if (remaining > BLOCK_LENGTH) {
                while (remaining > BLOCK_LENGTH) {
                    remaining -= BLOCK_LENGTH;
                    remainingBytes -= BLOCK_LENGTH;
                    lobMapIndex++;
                }
                bufferPos = 0;
                buffer = null;
            }
            fillBuffer();
            remaining -= skipSmall(remaining);
            remaining -= super.skip(remaining);
            return n - remaining;
        }

        private int skipSmall(long n) {
            if (buffer != null && bufferPos < buffer.length) {
                int x = MathUtils.convertLongToInt(Math.min(n, buffer.length - bufferPos));
                bufferPos += x;
                remainingBytes -= x;
                return x;
            }
            return 0;
        }

        @Override
        public int available() throws IOException {
            return MathUtils.convertLongToInt(remainingBytes);
        }

        @Override
        public int read(byte[] buff) throws IOException {
            return readFully(buff, 0, buff.length);
        }

        @Override
        public int read(byte[] buff, int off, int length) throws IOException {
            return readFully(buff, off, length);
        }

        private int readFully(byte[] buff, int off, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            int read = 0;
            while (length > 0) {
                fillBuffer();
                if (remainingBytes <= 0) {
                    break;
                }
                int len = (int) Math.min(length, remainingBytes);
                len = Math.min(len, buffer.length - bufferPos);
                System.arraycopy(buffer, bufferPos, buff, off, len);
                bufferPos += len;
                read += len;
                remainingBytes -= len;
                off += len;
                length -= len;
            }
            return read == 0 ? -1 : read;
        }

        private void fillBuffer() throws IOException {
            if (buffer != null && bufferPos < buffer.length) {
                return;
            }
            if (remainingBytes <= 0) {
                return;
            }
if (lobMapIndex >= lobMapBlocks.length) {
    System.out.println("halt!");
}
            try {
                buffer = readBlock(lobMapBlocks[lobMapIndex]);
                lobMapIndex++;
                bufferPos = 0;
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
        }

    }

}
