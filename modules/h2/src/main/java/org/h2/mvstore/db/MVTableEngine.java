/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.h2.api.ErrorCode;
import org.h2.api.TableEngine;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.store.InDoubtTransaction;
import org.h2.store.fs.FileChannelInputStream;
import org.h2.store.fs.FileUtils;
import org.h2.table.TableBase;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A table engine that internally uses the MVStore.
 */
public class MVTableEngine implements TableEngine {

    /**
     * Initialize the MVStore.
     *
     * @param db the database
     * @return the store
     */
    public static Store init(final Database db) {
        Store store = db.getStore();
        if (store != null) {
            return store;
        }
        byte[] key = db.getFileEncryptionKey();
        String dbPath = db.getDatabasePath();
        MVStore.Builder builder = new MVStore.Builder();
        store = new Store();
        boolean encrypted = false;
        if (dbPath != null) {
            String fileName = dbPath + Constants.SUFFIX_MV_FILE;
            MVStoreTool.compactCleanUp(fileName);
            builder.fileName(fileName);
            builder.pageSplitSize(db.getPageSize());
            if (db.isReadOnly()) {
                builder.readOnly();
            } else {
                // possibly create the directory
                boolean exists = FileUtils.exists(fileName);
                if (exists && !FileUtils.canWrite(fileName)) {
                    // read only
                } else {
                    String dir = FileUtils.getParent(fileName);
                    FileUtils.createDirectories(dir);
                }
            }
            if (key != null) {
                encrypted = true;
                builder.encryptionKey(decodePassword(key));
            }
            if (db.getSettings().compressData) {
                builder.compress();
                // use a larger page split size to improve the compression ratio
                builder.pageSplitSize(64 * 1024);
            }
            builder.backgroundExceptionHandler(new UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    db.setBackgroundException(DbException.convert(e));
                }

            });
        }
        store.open(db, builder, encrypted);
        db.setStore(store);
        return store;
    }

    /**
     * Convert password from byte[] to char[].
     *
     * @param key password as byte[]
     * @return password as char[].
     */
    static char[] decodePassword(byte[] key) {
        char[] password = new char[key.length / 2];
        for (int i = 0; i < password.length; i++) {
            password[i] = (char) (((key[i + i] & 255) << 16) |
                    ((key[i + i + 1]) & 255));
        }
        return password;
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        Store store = init(db);
        return store.createTable(data);
    }

    /**
     * A store with open tables.
     */
    public static class Store {

        /**
         * The map of open tables.
         * Key: the map name, value: the table.
         */
        private final ConcurrentHashMap<String, MVTable> tableMap =
                                                    new ConcurrentHashMap<>();

        /**
         * The store.
         */
        private MVStore mvStore;

        /**
         * The transaction store.
         */
        private TransactionStore transactionStore;

        private long statisticsStart;

        private int temporaryMapId;

        private boolean encrypted;

        private String fileName;

        /**
         * Open the store for this database.
         *
         * @param db the database
         * @param builder the builder
         * @param encrypted whether the store is encrypted
         */
        void open(Database db, MVStore.Builder builder, boolean encrypted) {
            this.encrypted = encrypted;
            try {
                this.mvStore = builder.open();
                FileStore fs = mvStore.getFileStore();
                if (fs != null) {
                    this.fileName = fs.getFileName();
                }
                if (!db.getSettings().reuseSpace) {
                    mvStore.setReuseSpace(false);
                }
                this.transactionStore = new TransactionStore(mvStore,
                        new ValueDataType(db, null), db.getLockTimeout());
            } catch (IllegalStateException e) {
                throw convertIllegalStateException(e);
            }
        }

        /**
         * Convert the illegal state exception to the correct database
         * exception.
         *
         * @param e the illegal state exception
         * @return the database exception
         */
        DbException convertIllegalStateException(IllegalStateException e) {
            int errorCode = DataUtils.getErrorCode(e.getMessage());
            if (errorCode == DataUtils.ERROR_FILE_CORRUPT) {
                if (encrypted) {
                    throw DbException.get(
                            ErrorCode.FILE_ENCRYPTION_ERROR_1,
                            e, fileName);
                }
            } else if (errorCode == DataUtils.ERROR_FILE_LOCKED) {
                throw DbException.get(
                        ErrorCode.DATABASE_ALREADY_OPEN_1,
                        e, fileName);
            } else if (errorCode == DataUtils.ERROR_READING_FAILED) {
                throw DbException.get(
                        ErrorCode.IO_EXCEPTION_1,
                        e, fileName);
            } else if (errorCode == DataUtils.ERROR_INTERNAL) {
                throw DbException.get(
                        ErrorCode.GENERAL_ERROR_1,
                        e, fileName);
            }
            throw DbException.get(
                    ErrorCode.FILE_CORRUPTED_1,
                    e, fileName);

        }

        public MVStore getMvStore() {
            return mvStore;
        }

        public TransactionStore getTransactionStore() {
            return transactionStore;
        }

        /**
         * Get MVTable by table name.
         *
         * @param tableName table name
         * @return MVTable
         */
        public MVTable getTable(String tableName) {
            return tableMap.get(tableName);
        }

        /**
         * Create a table.
         *
         * @param data CreateTableData
         * @return table created
         */
        public MVTable createTable(CreateTableData data) {
            MVTable table = new MVTable(data, this);
            tableMap.put(table.getMapName(), table);
            return table;
        }

        /**
         * Remove a table.
         *
         * @param table the table
         */
        public void removeTable(MVTable table) {
            tableMap.remove(table.getMapName());
        }

        /**
         * Store all pending changes.
         */
        public void flush() {
            FileStore s = mvStore.getFileStore();
            if (s == null || s.isReadOnly()) {
                return;
            }
            if (!mvStore.compact(50, 4 * 1024 * 1024)) {
                mvStore.commit();
            }
        }

        /**
         * Close the store, without persisting changes.
         */
        public void closeImmediately() {
            if (mvStore.isClosed()) {
                return;
            }
            mvStore.closeImmediately();
        }

        /**
         * Remove all temporary maps.
         *
         * @param objectIds the ids of the objects to keep
         */
        public void removeTemporaryMaps(BitSet objectIds) {
            for (String mapName : mvStore.getMapNames()) {
                if (mapName.startsWith("temp.")) {
                    mvStore.removeMap(mapName);
                } else if (mapName.startsWith("table.") || mapName.startsWith("index.")) {
                    int id = StringUtils.parseUInt31(mapName, mapName.indexOf('.') + 1, mapName.length());
                    if (!objectIds.get(id)) {
                        mvStore.removeMap(mapName);
                    }
                }
            }
        }

        /**
         * Get the name of the next available temporary map.
         *
         * @return the map name
         */
        public synchronized String nextTemporaryMapName() {
            return "temp." + temporaryMapId++;
        }

        /**
         * Prepare a transaction.
         *
         * @param session the session
         * @param transactionName the transaction name (may be null)
         */
        public void prepareCommit(Session session, String transactionName) {
            Transaction t = session.getTransaction();
            t.setName(transactionName);
            t.prepare();
            mvStore.commit();
        }

        public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
            List<Transaction> list = transactionStore.getOpenTransactions();
            ArrayList<InDoubtTransaction> result = Utils.newSmallArrayList();
            for (Transaction t : list) {
                if (t.getStatus() == Transaction.STATUS_PREPARED) {
                    result.add(new MVInDoubtTransaction(mvStore, t));
                }
            }
            return result;
        }

        /**
         * Set the maximum memory to be used by the cache.
         *
         * @param kb the maximum size in KB
         */
        public void setCacheSize(int kb) {
            mvStore.setCacheSize(Math.max(1, kb / 1024));
        }

        public InputStream getInputStream() {
            FileChannel fc = mvStore.getFileStore().getEncryptedFile();
            if (fc == null) {
                fc = mvStore.getFileStore().getFile();
            }
            return new FileChannelInputStream(fc, false);
        }

        /**
         * Force the changes to disk.
         */
        public void sync() {
            flush();
            mvStore.sync();
        }

        /**
         * Compact the database file, that is, compact blocks that have a low
         * fill rate, and move chunks next to each other. This will typically
         * shrink the database file. Changes are flushed to the file, and old
         * chunks are overwritten.
         *
         * @param maxCompactTime the maximum time in milliseconds to compact
         */
        public void compactFile(long maxCompactTime) {
            mvStore.setRetentionTime(0);
            long start = System.nanoTime();
            while (mvStore.compact(95, 16 * 1024 * 1024)) {
                mvStore.sync();
                mvStore.compactMoveChunks(95, 16 * 1024 * 1024);
                long time = System.nanoTime() - start;
                if (time > TimeUnit.MILLISECONDS.toNanos(maxCompactTime)) {
                    break;
                }
            }
        }

        /**
         * Close the store. Pending changes are persisted. Chunks with a low
         * fill rate are compacted, but old chunks are kept for some time, so
         * most likely the database file will not shrink.
         *
         * @param compactFully true if storage need to be compacted after closer
         */
        public void close(boolean compactFully) {
            try {
                FileStore fileStore = mvStore.getFileStore();
                if (!mvStore.isClosed() && fileStore != null) {
                    if (fileStore.isReadOnly()) {
                        compactFully = false;
                    } else {
                        transactionStore.close();
                    }
                    String fileName = fileStore.getFileName();
                    mvStore.close();
                    if (compactFully && FileUtils.exists(fileName)) {
                        // the file could have been deleted concurrently,
                        // so only compact if the file still exists
                        MVStoreTool.compact(fileName, true);
                    }
                }
            } catch (IllegalStateException e) {
                int errorCode = DataUtils.getErrorCode(e.getMessage());
                if (errorCode == DataUtils.ERROR_WRITING_FAILED) {
                    // disk full - ok
                } else if (errorCode == DataUtils.ERROR_FILE_CORRUPT) {
                    // wrong encryption key - ok
                }
                mvStore.closeImmediately();
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, "Closing");
            }
        }

        /**
         * Start collecting statistics.
         */
        public void statisticsStart() {
            FileStore fs = mvStore.getFileStore();
            statisticsStart = fs == null ? 0 : fs.getReadCount();
        }

        /**
         * Stop collecting statistics.
         *
         * @return the statistics
         */
        public Map<String, Integer> statisticsEnd() {
            HashMap<String, Integer> map = new HashMap<>();
            FileStore fs = mvStore.getFileStore();
            int reads = fs == null ? 0 : (int) (fs.getReadCount() - statisticsStart);
            map.put("reads", reads);
            return map;
        }

    }

    /**
     * An in-doubt transaction.
     */
    private static class MVInDoubtTransaction implements InDoubtTransaction {

        private final MVStore store;
        private final Transaction transaction;
        private int state = InDoubtTransaction.IN_DOUBT;

        MVInDoubtTransaction(MVStore store, Transaction transaction) {
            this.store = store;
            this.transaction = transaction;
        }

        @Override
        public void setState(int state) {
            if (state == InDoubtTransaction.COMMIT) {
                transaction.commit();
            } else {
                transaction.rollback();
            }
            store.commit();
            this.state = state;
        }

        @Override
        public String getState() {
            switch (state) {
            case IN_DOUBT:
                return "IN_DOUBT";
            case COMMIT:
                return "COMMIT";
            case ROLLBACK:
                return "ROLLBACK";
            default:
                throw DbException.throwInternalError("state="+state);
            }
        }

        @Override
        public String getTransactionName() {
            return transaction.getName();
        }

    }

}
