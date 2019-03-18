/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.New;

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public class TransactionStore {

    /**
     * The store.
     */
    final MVStore store;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    final MVMap<Integer, Object[]> preparedTransactions;

    /**
     * The undo log.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    final MVMap<Long, Object[]> undoLog;

    /**
     * the reader/writer lock for the undo-log. Allows us to process multiple
     * selects in parallel.
     */
    final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * The map of maps.
     */
    private final HashMap<Integer, MVMap<Object, VersionedValue>> maps =
            new HashMap<>();

    private final DataType dataType;

    private final BitSet openTransactions = new BitSet();

    private boolean init;

    private int maxTransactionId = 0xffff;

    /**
     * The next id of a temporary map.
     */
    private int nextTempMapId;

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new ObjectDataType());
    }

    /**
     * Create a new transaction store.
     *
     * @param store the store
     * @param dataType the data type for map keys and values
     */
    public TransactionStore(MVStore store, DataType dataType) {
        this.store = store;
        this.dataType = dataType;
        preparedTransactions = store.openMap("openTransactions",
                new MVMap.Builder<Integer, Object[]>());
        VersionedValueType oldValueType = new VersionedValueType(dataType);
        ArrayType undoLogValueType = new ArrayType(new DataType[]{
                new ObjectDataType(), dataType, oldValueType
        });
        MVMap.Builder<Long, Object[]> builder =
                new MVMap.Builder<Long, Object[]>().
                valueType(undoLogValueType);
        undoLog = store.openMap("undoLog", builder);
        if (undoLog.getValueType() != undoLogValueType) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TRANSACTION_CORRUPT,
                    "Undo map open with a different value type");
        }
    }

    /**
     * Initialize the store. This is needed before a transaction can be opened.
     * If the transaction store is corrupt, this method can throw an exception,
     * in which case the store can only be used for reading.
     */
    public synchronized void init() {
        init = true;
        // remove all temporary maps
        for (String mapName : store.getMapNames()) {
            if (mapName.startsWith("temp.")) {
                MVMap<Object, Integer> temp = openTempMap(mapName);
                store.removeMap(temp);
            }
        }
        rwLock.writeLock().lock();
        try {
            if (undoLog.size() > 0) {
                for (Long key : undoLog.keySet()) {
                    int transactionId = getTransactionId(key);
                    openTransactions.set(transactionId);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Set the maximum transaction id, after which ids are re-used. If the old
     * transaction is still in use when re-using an old id, the new transaction
     * fails.
     *
     * @param max the maximum id
     */
    public void setMaxTransactionId(int max) {
        this.maxTransactionId = max;
    }

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << 24),
                "Transaction id out of range: {0}", transactionId);
        DataUtils.checkArgument(logId >= 0 && logId < (1L << 40),
                "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << 40) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    static int getTransactionId(long operationId) {
        return (int) (operationId >>> 40);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & ((1L << 40) - 1);
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    public List<Transaction> getOpenTransactions() {
        rwLock.readLock().lock();
        try {
            ArrayList<Transaction> list = New.arrayList();
            Long key = undoLog.firstKey();
            while (key != null) {
                int transactionId = getTransactionId(key);
                key = undoLog.lowerKey(getOperationId(transactionId + 1, 0));
                long logId = getLogId(key) + 1;
                Object[] data = preparedTransactions.get(transactionId);
                int status;
                String name;
                if (data == null) {
                    if (undoLog.containsKey(getOperationId(transactionId, 0))) {
                        status = Transaction.STATUS_OPEN;
                    } else {
                        status = Transaction.STATUS_COMMITTING;
                    }
                    name = null;
                } else {
                    status = (Integer) data[0];
                    name = (String) data[1];
                }
                Transaction t = new Transaction(this, transactionId, status,
                        name, logId);
                list.add(t);
                key = undoLog.ceilingKey(getOperationId(transactionId + 1, 0));
            }
            return list;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        store.commit();
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public synchronized Transaction begin() {

        int transactionId;
        int status;
        if (!init) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                    "Not initialized");
        }
        transactionId = openTransactions.nextClearBit(1);
        if (transactionId > maxTransactionId) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                    "There are {0} open transactions",
                    transactionId - 1);
        }
        openTransactions.set(transactionId);
        status = Transaction.STATUS_OPEN;
        return new Transaction(this, transactionId, status, null, 0);
    }

    /**
     * Store a transaction.
     *
     * @param t the transaction
     */
    synchronized void storeTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED ||
                t.getName() != null) {
            Object[] v = { t.getStatus(), t.getName() };
            preparedTransactions.put(t.getId(), v);
        }
    }

    /**
     * Log an entry.
     *
     * @param t the transaction
     * @param logId the log id
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    void log(Transaction t, long logId, int mapId,
            Object key, Object oldValue) {
        Long undoKey = getOperationId(t.getId(), logId);
        Object[] log = { mapId, key, oldValue };
        rwLock.writeLock().lock();
        try {
            if (logId == 0) {
                if (undoLog.containsKey(undoKey)) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                            "An old transaction with the same id " +
                            "is still open: {0}",
                            t.getId());
                }
            }
            undoLog.put(undoKey, log);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Remove a log entry.
     *
     * @param t the transaction
     * @param logId the log id
     */
    public void logUndo(Transaction t, long logId) {
        Long undoKey = getOperationId(t.getId(), logId);
        rwLock.writeLock().lock();
        try {
            Object[] old = undoLog.remove(undoKey);
            if (old == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction {0} was concurrently rolled back",
                        t.getId());
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Remove the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    synchronized <K, V> void removeMap(TransactionMap<K, V> map) {
        maps.remove(map.mapId);
        store.removeMap(map.map);
    }

    /**
     * Commit a transaction.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     */
    void commit(Transaction t, long maxLogId) {
        if (store.isClosed()) {
            return;
        }
        // TODO could synchronize on blocks (100 at a time or so)
        rwLock.writeLock().lock();
        int oldStatus = t.getStatus();
        try {
            t.setStatus(Transaction.STATUS_COMMITTING);
            for (long logId = 0; logId < maxLogId; logId++) {
                Long undoKey = getOperationId(t.getId(), logId);
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially committed: load next
                    undoKey = undoLog.ceilingKey(undoKey);
                    if (undoKey == null ||
                            getTransactionId(undoKey) != t.getId()) {
                        break;
                    }
                    logId = getLogId(undoKey) - 1;
                    continue;
                }
                int mapId = (Integer) op[0];
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map != null) { // might be null if map was removed later
                    Object key = op[1];
                    VersionedValue value = map.get(key);
                    if (value != null) {
                        // only commit (remove/update) value if we've reached
                        // last undoLog entry for a given key
                        if (value.operationId == undoKey) {
                            if (value.value == null) {
                                map.remove(key);
                            } else {
                                map.put(key, new VersionedValue(0L, value.value));
                            }
                        }
                    }
                }
                undoLog.remove(undoKey);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
        endTransaction(t, oldStatus);
    }

    /**
     * Open the map with the given name.
     *
     * @param <K> the key type
     * @param name the map name
     * @param keyType the key type
     * @param valueType the value type
     * @return the map
     */
    synchronized <K> MVMap<K, VersionedValue> openMap(String name,
            DataType keyType, DataType valueType) {
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        VersionedValueType vt = new VersionedValueType(valueType);
        MVMap<K, VersionedValue> map;
        MVMap.Builder<K, VersionedValue> builder =
                new MVMap.Builder<K, VersionedValue>().
                keyType(keyType).valueType(vt);
        map = store.openMap(name, builder);
        @SuppressWarnings("unchecked")
        MVMap<Object, VersionedValue> m = (MVMap<Object, VersionedValue>) map;
        maps.put(map.getId(), m);
        return map;
    }

    /**
     * Open the map with the given id.
     *
     * @param mapId the id
     * @return the map
     */
    synchronized MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = maps.get(mapId);
        if (map != null) {
            return map;
        }
        String mapName = store.getMapName(mapId);
        if (mapName == null) {
            // the map was removed later on
            return null;
        }
        VersionedValueType vt = new VersionedValueType(dataType);
        MVMap.Builder<Object, VersionedValue> mapBuilder =
                new MVMap.Builder<Object, VersionedValue>().
                keyType(dataType).valueType(vt);
        map = store.openMap(mapName, mapBuilder);
        maps.put(mapId, map);
        return map;
    }

    /**
     * Create a temporary map. Such maps are removed when opening the store.
     *
     * @return the map
     */
    synchronized MVMap<Object, Integer> createTempMap() {
        String mapName = "temp." + nextTempMapId++;
        return openTempMap(mapName);
    }

    /**
     * Open a temporary map.
     *
     * @param mapName the map name
     * @return the map
     */
    MVMap<Object, Integer> openTempMap(String mapName) {
        MVMap.Builder<Object, Integer> mapBuilder =
                new MVMap.Builder<Object, Integer>().
                keyType(dataType);
        return store.openMap(mapName, mapBuilder);
    }

    /**
     * End this transaction
     *
     * @param t the transaction
     * @param oldStatus status of this transaction
     */
    synchronized void endTransaction(Transaction t, int oldStatus) {
        if (oldStatus == Transaction.STATUS_PREPARED) {
            preparedTransactions.remove(t.getId());
        }
        t.setStatus(Transaction.STATUS_CLOSED);
        openTransactions.clear(t.transactionId);
        if (oldStatus == Transaction.STATUS_PREPARED || store.getAutoCommitDelay() == 0) {
            store.commit();
            return;
        }
        // to avoid having to store the transaction log,
        // if there is no open transaction,
        // and if there have been many changes, store them now
        if (undoLog.isEmpty()) {
            int unsaved = store.getUnsavedMemory();
            int max = store.getAutoCommitMemory();
            // save at 3/4 capacity
            if (unsaved * 4 > max * 3) {
                store.commit();
            }
        }
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        // TODO could synchronize on blocks (100 at a time or so)
        rwLock.writeLock().lock();
        try {
            for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
                Long undoKey = getOperationId(t.getId(), logId);
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially rolled back: load previous
                    undoKey = undoLog.floorKey(undoKey);
                    if (undoKey == null ||
                            getTransactionId(undoKey) != t.getId()) {
                        break;
                    }
                    logId = getLogId(undoKey) + 1;
                    continue;
                }
                int mapId = ((Integer) op[0]).intValue();
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map != null) {
                    Object key = op[1];
                    VersionedValue oldValue = (VersionedValue) op[2];
                    if (oldValue == null) {
                        // this transaction added the value
                        map.remove(key);
                    } else {
                        // this transaction updated the value
                        map.put(key, oldValue);
                    }
                }
                undoLog.remove(undoKey);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Get the changes of the given transaction, starting from the latest log id
     * back to the given log id.
     *
     * @param t the transaction
     * @param maxLogId the maximum log id
     * @param toLogId the minimum log id
     * @return the changes
     */
    Iterator<Change> getChanges(final Transaction t, final long maxLogId,
            final long toLogId) {
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            {
                fetchNext();
            }

            private void fetchNext() {
                rwLock.writeLock().lock();
                try {
                    while (logId >= toLogId) {
                        Long undoKey = getOperationId(t.getId(), logId);
                        Object[] op = undoLog.get(undoKey);
                        logId--;
                        if (op == null) {
                            // partially rolled back: load previous
                            undoKey = undoLog.floorKey(undoKey);
                            if (undoKey == null ||
                                    getTransactionId(undoKey) != t.getId()) {
                                break;
                            }
                            logId = getLogId(undoKey);
                            continue;
                        }
                        int mapId = ((Integer) op[0]).intValue();
                        MVMap<Object, VersionedValue> m = openMap(mapId);
                        if (m == null) {
                            // map was removed later on
                        } else {
                            current = new Change();
                            current.mapName = m.getName();
                            current.key = op[1];
                            VersionedValue oldValue = (VersionedValue) op[2];
                            current.value = oldValue == null ?
                                    null : oldValue.value;
                            return;
                        }
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Change next() {
                if (current == null) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

    /**
     * A change in a map.
     */
    public static class Change {

        /**
         * The name of the map where the change occurred.
         */
        public String mapName;

        /**
         * The key.
         */
        public Object key;

        /**
         * The value.
         */
        public Object value;
    }

    /**
     * A transaction.
     */
    public static class Transaction {

        /**
         * The status of a closed transaction (committed or rolled back).
         */
        public static final int STATUS_CLOSED = 0;

        /**
         * The status of an open transaction.
         */
        public static final int STATUS_OPEN = 1;

        /**
         * The status of a prepared transaction.
         */
        public static final int STATUS_PREPARED = 2;

        /**
         * The status of a transaction that is being committed, but possibly not
         * yet finished. A transactions can go into this state when the store is
         * closed while the transaction is committing. When opening a store,
         * such transactions should be committed.
         */
        public static final int STATUS_COMMITTING = 3;

        /**
         * The transaction store.
         */
        final TransactionStore store;

        /**
         * The transaction id.
         */
        final int transactionId;

        /**
         * The log id of the last entry in the undo log map.
         */
        long logId;

        private int status;

        private String name;

        Transaction(TransactionStore store, int transactionId, int status,
                String name, long logId) {
            this.store = store;
            this.transactionId = transactionId;
            this.status = status;
            this.name = name;
            this.logId = logId;
        }

        public int getId() {
            return transactionId;
        }

        public int getStatus() {
            return status;
        }

        void setStatus(int status) {
            this.status = status;
        }

        public void setName(String name) {
            checkNotClosed();
            this.name = name;
            store.storeTransaction(this);
        }

        public String getName() {
            return name;
        }

        /**
         * Create a new savepoint.
         *
         * @return the savepoint id
         */
        public long setSavepoint() {
            return logId;
        }

        /**
         * Add a log entry.
         *
         * @param mapId the map id
         * @param key the key
         * @param oldValue the old value
         */
        void log(int mapId, Object key, Object oldValue) {
            store.log(this, logId, mapId, key, oldValue);
            // only increment the log id if logging was successful
            logId++;
        }

        /**
         * Remove the last log entry.
         */
        void logUndo() {
            store.logUndo(this, --logId);
        }

        /**
         * Open a data map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name) {
            return openMap(name, null, null);
        }

        /**
         * Open the map to store the data.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @param keyType the key data type
         * @param valueType the value data type
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name,
                DataType keyType, DataType valueType) {
            checkNotClosed();
            MVMap<K, VersionedValue> map = store.openMap(name, keyType,
                    valueType);
            int mapId = map.getId();
            return new TransactionMap<>(this, map, mapId);
        }

        /**
         * Open the transactional version of the given map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param map the base map
         * @return the transactional map
         */
        public <K, V> TransactionMap<K, V> openMap(
                MVMap<K, VersionedValue> map) {
            checkNotClosed();
            int mapId = map.getId();
            return new TransactionMap<>(this, map, mapId);
        }

        /**
         * Prepare the transaction. Afterwards, the transaction can only be
         * committed or rolled back.
         */
        public void prepare() {
            checkNotClosed();
            status = STATUS_PREPARED;
            store.storeTransaction(this);
        }

        /**
         * Commit the transaction. Afterwards, this transaction is closed.
         */
        public void commit() {
            checkNotClosed();
            store.commit(this, logId);
        }

        /**
         * Roll back to the given savepoint. This is only allowed if the
         * transaction is open.
         *
         * @param savepointId the savepoint id
         */
        public void rollbackToSavepoint(long savepointId) {
            checkNotClosed();
            store.rollbackTo(this, logId, savepointId);
            logId = savepointId;
        }

        /**
         * Roll the transaction back. Afterwards, this transaction is closed.
         */
        public void rollback() {
            checkNotClosed();
            store.rollbackTo(this, logId, 0);
            store.endTransaction(this, status);
        }

        /**
         * Get the list of changes, starting with the latest change, up to the
         * given savepoint (in reverse order than they occurred). The value of
         * the change is the value before the change was applied.
         *
         * @param savepointId the savepoint id, 0 meaning the beginning of the
         *            transaction
         * @return the changes
         */
        public Iterator<Change> getChanges(long savepointId) {
            return store.getChanges(this, logId, savepointId);
        }

        /**
         * Check whether this transaction is open or prepared.
         */
        void checkNotClosed() {
            if (status == STATUS_CLOSED) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CLOSED, "Transaction is closed");
            }
        }

        /**
         * Remove the map.
         *
         * @param map the map
         */
        public <K, V> void removeMap(TransactionMap<K, V> map) {
            store.removeMap(map);
        }

        @Override
        public String toString() {
            return "" + transactionId;
        }

    }

    /**
     * A map that supports transactions.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class TransactionMap<K, V> {

        /**
         * The map id.
         */
        final int mapId;

        /**
         * If a record was read that was updated by this transaction, and the
         * update occurred before this log id, the older version is read. This
         * is so that changes are not immediately visible, to support statement
         * processing (for example "update test set id = id + 1").
         */
        long readLogId = Long.MAX_VALUE;

        /**
         * The map used for writing (the latest version).
         * <p>
         * Key: key the key of the data.
         * Value: { transactionId, oldVersion, value }
         */
        final MVMap<K, VersionedValue> map;

        /**
         * The transaction which is used for this map.
         */
        final Transaction transaction;

        TransactionMap(Transaction transaction, MVMap<K, VersionedValue> map,
                int mapId) {
            this.transaction = transaction;
            this.map = map;
            this.mapId = mapId;
        }

        /**
         * Set the savepoint. Afterwards, reads are based on the specified
         * savepoint.
         *
         * @param savepoint the savepoint
         */
        public void setSavepoint(long savepoint) {
            this.readLogId = savepoint;
        }

        /**
         * Get a clone of this map for the given transaction.
         *
         * @param transaction the transaction
         * @param savepoint the savepoint
         * @return the map
         */
        public TransactionMap<K, V> getInstance(Transaction transaction,
                long savepoint) {
            TransactionMap<K, V> m =
                    new TransactionMap<>(transaction, map, mapId);
            m.setSavepoint(savepoint);
            return m;
        }

        /**
         * Get the size of the raw map. This includes uncommitted entries, and
         * transiently removed entries, so it is the maximum number of entries.
         *
         * @return the maximum size
         */
        public long sizeAsLongMax() {
            return map.sizeAsLong();
        }

        /**
         * Get the size of the map as seen by this transaction.
         *
         * @return the size
         */
        public long sizeAsLong() {
            transaction.store.rwLock.readLock().lock();
            try {
                long sizeRaw = map.sizeAsLong();
                MVMap<Long, Object[]> undo = transaction.store.undoLog;
                long undoLogSize;
                synchronized (undo) {
                    undoLogSize = undo.sizeAsLong();
                }
                if (undoLogSize == 0) {
                    return sizeRaw;
                }
                if (undoLogSize > sizeRaw) {
                    // the undo log is larger than the map -
                    // count the entries of the map
                    long size = 0;
                    Cursor<K, VersionedValue> cursor = map.cursor(null);
                    while (cursor.hasNext()) {
                        K key = cursor.next();
                        // cursor.getValue() returns outdated value
                        VersionedValue data = map.get(key);
                        data = getValue(key, readLogId, data);
                        if (data != null && data.value != null) {
                            size++;
                        }
                    }
                    return size;
                }
                // the undo log is smaller than the map -
                // scan the undo log and subtract invisible entries
                synchronized (undo) {
                    // re-fetch in case any transaction was committed now
                    long size = map.sizeAsLong();
                    MVMap<Object, Integer> temp = transaction.store
                            .createTempMap();
                    try {
                        for (Entry<Long, Object[]> e : undo.entrySet()) {
                            Object[] op = e.getValue();
                            int m = (Integer) op[0];
                            if (m != mapId) {
                                // a different map - ignore
                                continue;
                            }
                            @SuppressWarnings("unchecked")
                            K key = (K) op[1];
                            if (get(key) == null) {
                                Integer old = temp.put(key, 1);
                                // count each key only once (there might be
                                // multiple
                                // changes for the same key)
                                if (old == null) {
                                    size--;
                                }
                            }
                        }
                    } finally {
                        transaction.store.store.removeMap(temp);
                    }
                    return size;
                }
            } finally {
                transaction.store.rwLock.readLock().unlock();
            }
        }

        /**
         * Remove an entry.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V remove(K key) {
            return set(key, null);
        }

        /**
         * Update the value for the given key.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @param value the new value (not null)
         * @return the old value
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V put(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return set(key, value);
        }

        /**
         * Update the value for the given key, without adding an undo log entry.
         *
         * @param key the key
         * @param value the value
         * @return the old value
         */
        @SuppressWarnings("unchecked")
        public V putCommitted(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            VersionedValue newValue = new VersionedValue(0L, value);
            VersionedValue oldValue = map.put(key, newValue);
            return (V) (oldValue == null ? null : oldValue.value);
        }

        private V set(K key, V value) {
            transaction.checkNotClosed();
            V old = get(key);
            boolean ok = trySet(key, value, false);
            if (ok) {
                return old;
            }
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
        }

        /**
         * Try to remove the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @return whether the entry could be removed
         */
        public boolean tryRemove(K key) {
            return trySet(key, null, false);
        }

        /**
         * Try to update the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @param value the new value
         * @return whether the entry could be updated
         */
        public boolean tryPut(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return trySet(key, value, false);
        }

        /**
         * Try to set or remove the value. When updating only unchanged entries,
         * then the value is only changed if it was not changed after opening
         * the map.
         *
         * @param key the key
         * @param value the new value (null to remove the value)
         * @param onlyIfUnchanged only set the value if it was not changed (by
         *            this or another transaction) since the map was opened
         * @return true if the value was set, false if there was a concurrent
         *         update
         */
        public boolean trySet(K key, V value, boolean onlyIfUnchanged) {
            VersionedValue current = map.get(key);
            if (onlyIfUnchanged) {
                VersionedValue old = getValue(key, readLogId);
                if (!map.areValuesEqual(old, current)) {
                    long tx = getTransactionId(current.operationId);
                    if (tx == transaction.transactionId) {
                        if (value == null) {
                            // ignore removing an entry
                            // if it was added or changed
                            // in the same statement
                            return true;
                        } else if (current.value == null) {
                            // add an entry that was removed
                            // in the same statement
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
            VersionedValue newValue = new VersionedValue(
                    getOperationId(transaction.transactionId, transaction.logId),
                    value);
            if (current == null) {
                // a new value
                transaction.log(mapId, key, current);
                VersionedValue old = map.putIfAbsent(key, newValue);
                if (old != null) {
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            long id = current.operationId;
            if (id == 0) {
                // committed
                transaction.log(mapId, key, current);
                // the transaction is committed:
                // overwrite the value
                if (!map.replace(key, current, newValue)) {
                    // somebody else was faster
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            int tx = getTransactionId(current.operationId);
            if (tx == transaction.transactionId) {
                // added or updated by this transaction
                transaction.log(mapId, key, current);
                if (!map.replace(key, current, newValue)) {
                    // strange, somebody overwrote the value
                    // even though the change was not committed
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            // the transaction is not yet committed
            return false;
        }

        /**
         * Get the value for the given key at the time when this map was opened.
         *
         * @param key the key
         * @return the value or null
         */
        public V get(K key) {
            return get(key, readLogId);
        }

        /**
         * Get the most recent value for the given key.
         *
         * @param key the key
         * @return the value or null
         */
        public V getLatest(K key) {
            return get(key, Long.MAX_VALUE);
        }

        /**
         * Whether the map contains the key.
         *
         * @param key the key
         * @return true if the map contains an entry for this key
         */
        public boolean containsKey(K key) {
            return get(key) != null;
        }

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @param maxLogId the maximum log id
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public V get(K key, long maxLogId) {
            VersionedValue data = getValue(key, maxLogId);
            return data == null ? null : (V) data.value;
        }

        /**
         * Whether the entry for this key was added or removed from this
         * session.
         *
         * @param key the key
         * @return true if yes
         */
        public boolean isSameTransaction(K key) {
            VersionedValue data = map.get(key);
            if (data == null) {
                // doesn't exist or deleted by a committed transaction
                return false;
            }
            int tx = getTransactionId(data.operationId);
            return tx == transaction.transactionId;
        }

        private VersionedValue getValue(K key, long maxLog) {
            transaction.store.rwLock.readLock().lock();
            try {
                VersionedValue data = map.get(key);
                return getValue(key, maxLog, data);
            } finally {
                transaction.store.rwLock.readLock().unlock();
            }
        }

        /**
         * Get the versioned value for the given key.
         *
         * @param key the key
         * @param maxLog the maximum log id of the entry
         * @param data the value stored in the main map
         * @return the value
         */
        VersionedValue getValue(K key, long maxLog, VersionedValue data) {
            while (true) {
                if (data == null) {
                    // doesn't exist or deleted by a committed transaction
                    return null;
                }
                long id = data.operationId;
                if (id == 0) {
                    // it is committed
                    return data;
                }
                int tx = getTransactionId(id);
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    if (getLogId(id) < maxLog) {
                        return data;
                    }
                }
                // get the value before the uncommitted transaction
                Object[] d;
                d = transaction.store.undoLog.get(id);
                if (d == null) {
                    if (transaction.store.store.isReadOnly()) {
                        // uncommitted transaction for a read-only store
                        return null;
                    }
                    // this entry should be committed or rolled back
                    // in the meantime (the transaction might still be open)
                    // or it might be changed again in a different
                    // transaction (possibly one with the same id)
                    data = map.get(key);
                } else {
                    data = (VersionedValue) d[2];
                }
            }
        }

        /**
         * Check whether this map is closed.
         *
         * @return true if closed
         */
        public boolean isClosed() {
            return map.isClosed();
        }

        /**
         * Clear the map.
         */
        public void clear() {
            // TODO truncate transactionally?
            map.clear();
        }

        /**
         * Get the first key.
         *
         * @return the first key, or null if empty
         */
        public K firstKey() {
            Iterator<K> it = keyIterator(null);
            return it.hasNext() ? it.next() : null;
        }

        /**
         * Get the last key.
         *
         * @return the last key, or null if empty
         */
        public K lastKey() {
            K k = map.lastKey();
            while (k != null && get(k) == null) {
                k = map.lowerKey(k);
            }
            return k;
        }

        /**
         * Get the smallest key that is larger than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K higherKey(K key) {
            do {
                key = map.higherKey(key);
            } while (key != null && get(key) == null);
            return key;
        }

        /**
         * Get the smallest key that is larger than or equal to this key,
         * or null if no such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K ceilingKey(K key) {
            Iterator<K> it = keyIterator(key);
            return it.hasNext() ? it.next() : null;
        }

        /**
         * Get one of the previous or next keys. There might be no value
         * available for the returned key.
         *
         * @param key the key (may not be null)
         * @param offset how many keys to skip (-1 for previous, 1 for next)
         * @return the key
         */
        public K relativeKey(K key, long offset) {
            K k = offset > 0 ? map.ceilingKey(key) : map.floorKey(key);
            if (k == null) {
                return k;
            }
            long index = map.getKeyIndex(k);
            return map.getKey(index + offset);
        }

        /**
         * Get the largest key that is smaller than or equal to this key,
         * or null if no such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K floorKey(K key) {
            key = map.floorKey(key);
            while (key != null && get(key) == null) {
                // Use lowerKey() for the next attempts, otherwise we'll get an infinite loop
                key = map.lowerKey(key);
            }
            return key;
        }

        /**
         * Get the largest key that is smaller than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K lowerKey(K key) {
            do {
                key = map.lowerKey(key);
            } while (key != null && get(key) == null);
            return key;
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from) {
            return keyIterator(from, false);
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @param includeUncommitted whether uncommitted entries should be
         *            included
         * @return the iterator
         */
        public Iterator<K> keyIterator(final K from, final boolean includeUncommitted) {
            return new Iterator<K>() {
                private K currentKey = from;
                private Cursor<K, VersionedValue> cursor = map.cursor(currentKey);

                {
                    fetchNext();
                }

                private void fetchNext() {
                    while (cursor.hasNext()) {
                        K k;
                        try {
                            k = cursor.next();
                        } catch (IllegalStateException e) {
                            // TODO this is a bit ugly
                            if (DataUtils.getErrorCode(e.getMessage()) ==
                                    DataUtils.ERROR_CHUNK_NOT_FOUND) {
                                cursor = map.cursor(currentKey);
                                // we (should) get the current key again,
                                // we need to ignore that one
                                if (!cursor.hasNext()) {
                                    break;
                                }
                                cursor.next();
                                if (!cursor.hasNext()) {
                                    break;
                                }
                                k = cursor.next();
                            } else {
                                throw e;
                            }
                        }
                        currentKey = k;
                        if (includeUncommitted) {
                            return;
                        }
                        if (containsKey(k)) {
                            return;
                        }
                    }
                    currentKey = null;
                }

                @Override
                public boolean hasNext() {
                    return currentKey != null;
                }

                @Override
                public K next() {
                    K result = currentKey;
                    fetchNext();
                    return result;
                }

                @Override
                public void remove() {
                    throw DataUtils.newUnsupportedOperationException(
                            "Removing is not supported");
                }
            };
        }

        /**
         * Iterate over entries.
         *
         * @param from the first key to return
         * @param to the last key to return
         * @return the iterator
         */
        public Iterator<Entry<K, V>> entryIterator(final K from, final K to) {
            return new Iterator<Entry<K, V>>() {
                private Entry<K, V> current;
                private K currentKey = from;
                private Cursor<K, VersionedValue> cursor = map.cursor(currentKey);

                {
                    fetchNext();
                }

                private void fetchNext() {
                    while (cursor.hasNext()) {
                        transaction.store.rwLock.readLock().lock();
                        try {
                            K k;
                            try {
                                k = cursor.next();
                            } catch (IllegalStateException e) {
                                // TODO this is a bit ugly
                                if (DataUtils.getErrorCode(e.getMessage()) ==
                                        DataUtils.ERROR_CHUNK_NOT_FOUND) {
                                    cursor = map.cursor(currentKey);
                                    // we (should) get the current key again,
                                    // we need to ignore that one
                                    if (!cursor.hasNext()) {
                                        break;
                                    }
                                    cursor.next();
                                    if (!cursor.hasNext()) {
                                        break;
                                    }
                                    k = cursor.next();
                                } else {
                                    throw e;
                                }
                            }
                            final K key = k;
                            if (to != null && map.getKeyType().compare(k, to) > 0) {
                                break;
                            }
                            // cursor.getValue() returns outdated value
                            VersionedValue data = map.get(key);
                            data = getValue(key, readLogId, data);
                            if (data != null && data.value != null) {
                                @SuppressWarnings("unchecked")
                                final V value = (V) data.value;
                                current = new DataUtils.MapEntry<>(key, value);
                                currentKey = key;
                                return;
                            }
                        } finally {
                            transaction.store.rwLock.readLock().unlock();
                        }
                    }
                    current = null;
                    currentKey = null;
                }

                @Override
                public boolean hasNext() {
                    return current != null;
                }

                @Override
                public Entry<K, V> next() {
                    Entry<K, V> result = current;
                    fetchNext();
                    return result;
                }

                @Override
                public void remove() {
                    throw DataUtils.newUnsupportedOperationException(
                            "Removing is not supported");
                }
            };

        }

        /**
         * Iterate over keys.
         *
         * @param iterator the iterator to wrap
         * @param includeUncommitted whether uncommitted entries should be
         *            included
         * @return the iterator
         */
        public Iterator<K> wrapIterator(final Iterator<K> iterator,
                final boolean includeUncommitted) {
            // TODO duplicate code for wrapIterator and entryIterator
            return new Iterator<K>() {
                private K current;

                {
                    fetchNext();
                }

                private void fetchNext() {
                    while (iterator.hasNext()) {
                        current = iterator.next();
                        if (includeUncommitted) {
                            return;
                        }
                        if (containsKey(current)) {
                            return;
                        }
                    }
                    current = null;
                }

                @Override
                public boolean hasNext() {
                    return current != null;
                }

                @Override
                public K next() {
                    K result = current;
                    fetchNext();
                    return result;
                }

                @Override
                public void remove() {
                    throw DataUtils.newUnsupportedOperationException(
                            "Removing is not supported");
                }
            };
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public DataType getKeyType() {
            return map.getKeyType();
        }

    }

    /**
     * A versioned value (possibly null). It contains a pointer to the old
     * value, and the value itself.
     */
    static class VersionedValue {

        /**
         * The operation id.
         */
        final long operationId;

        /**
         * The value.
         */
        final Object value;

        VersionedValue(long operationId, Object value) {
            this.operationId = operationId;
            this.value = value;
        }

        @Override
        public String toString() {
            return value + (operationId == 0 ? "" : (
                    " " +
                    getTransactionId(operationId) + "/" +
                    getLogId(operationId)));
        }

    }

    /**
     * The value type for a versioned value.
     */
    public static class VersionedValueType implements DataType {

        private final DataType valueType;

        VersionedValueType(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            VersionedValue v = (VersionedValue) obj;
            return valueType.getMemory(v.value) + 8;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            VersionedValue a = (VersionedValue) aObj;
            VersionedValue b = (VersionedValue) bObj;
            long comp = a.operationId - b.operationId;
            if (comp == 0) {
                return valueType.compare(a.value, b.value);
            }
            return Long.signum(comp);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            if (buff.get() == 0) {
                // fast path (no op ids or null entries)
                for (int i = 0; i < len; i++) {
                    obj[i] = new VersionedValue(0L, valueType.read(buff));
                }
            } else {
                // slow path (some entries may be null)
                for (int i = 0; i < len; i++) {
                    obj[i] = read(buff);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            long operationId = DataUtils.readVarLong(buff);
            Object value;
            if (buff.get() == 1) {
                value = valueType.read(buff);
            } else {
                value = null;
            }
            return new VersionedValue(operationId, value);
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            boolean fastPath = true;
            for (int i = 0; i < len; i++) {
                VersionedValue v = (VersionedValue) obj[i];
                if (v.operationId != 0 || v.value == null) {
                    fastPath = false;
                }
            }
            if (fastPath) {
                buff.put((byte) 0);
                for (int i = 0; i < len; i++) {
                    VersionedValue v = (VersionedValue) obj[i];
                    valueType.write(buff, v.value);
                }
            } else {
                // slow path:
                // store op ids, and some entries may be null
                buff.put((byte) 1);
                for (int i = 0; i < len; i++) {
                    write(buff, obj[i]);
                }
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            buff.putVarLong(v.operationId);
            if (v.value == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                valueType.write(buff, v.value);
            }
        }

    }

    /**
     * A data type that contains an array of objects with the specified data
     * types.
     */
    public static class ArrayType implements DataType {

        private final int arrayLength;
        private final DataType[] elementTypes;

        ArrayType(DataType[] elementTypes) {
            this.arrayLength = elementTypes.length;
            this.elementTypes = elementTypes;
        }

        @Override
        public int getMemory(Object obj) {
            Object[] array = (Object[]) obj;
            int size = 0;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                Object o = array[i];
                if (o != null) {
                    size += t.getMemory(o);
                }
            }
            return size;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            Object[] a = (Object[]) aObj;
            Object[] b = (Object[]) bObj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                int comp = t.compare(a[i], b[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Object[] array = (Object[]) obj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                Object o = array[i];
                if (o == null) {
                    buff.put((byte) 0);
                } else {
                    buff.put((byte) 1);
                    t.write(buff, o);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            Object[] array = new Object[arrayLength];
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                if (buff.get() == 1) {
                    array[i] = t.read(buff);
                }
            }
            return array;
        }

    }

}
