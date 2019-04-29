/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.StringUtils;
import org.h2.value.VersionedValue;

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public class TransactionStore {

    /**
     * The store.
     */
    final MVStore store;

    /**
     * Default blocked transaction timeout
     */
    private final int timeoutMillis;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    private final MVMap<Integer, Object[]> preparedTransactions;

    /**
     * Undo logs.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    @SuppressWarnings("unchecked")
    final MVMap<Long,Object[]> undoLogs[] = new MVMap[MAX_OPEN_TRANSACTIONS];
    private final MVMap.Builder<Long,Object[]> undoLogBuilder;

    private final MVMap.Builder<Object, VersionedValue> mapBuilder;

    /**
     * This BitSet is used as vacancy indicator for transaction slots in transactions[].
     * It provides easy way to find first unoccupied slot, and also allows for copy-on-write
     * non-blocking updates.
     */
    final AtomicReference<VersionedBitSet> openTransactions = new AtomicReference<>(new VersionedBitSet());

    /**
     * This is intended to be the source of ultimate truth about transaction being committed.
     * Once bit is set, corresponding transaction is logically committed,
     * although it might be plenty of "uncommitted" entries in various maps
     * and undo record are still around.
     * Nevertheless, all of those should be considered by other transactions as committed.
     */
    final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());

    private boolean init;

    /**
     * Soft limit on the number of concurrently opened transactions.
     * Not really needed but used by some test.
     */
    private int maxTransactionId = MAX_OPEN_TRANSACTIONS;

    /**
     * Array holding all open transaction objects.
     * Position in array is "transaction id".
     * VolatileReferenceArray would do the job here, but there is no such thing in Java yet
     */
    private final AtomicReferenceArray<Transaction> transactions =
                                                        new AtomicReferenceArray<>(MAX_OPEN_TRANSACTIONS + 1);

    private static final String UNDO_LOG_NAME_PREFIX = "undoLog";
    private static final char UNDO_LOG_COMMITTED = '-'; // must come before open in lexicographical order
    private static final char UNDO_LOG_OPEN = '.';

    /**
     * Hard limit on the number of concurrently opened transactions
     */
    // TODO: introduce constructor parameter instead of a static field, driven by URL parameter
    private static final int MAX_OPEN_TRANSACTIONS = 65535;


    /**
     * Generate a string used to name undo log map for a specific transaction.
     * This name will contain transaction id and reflect the fact
     * whether transaction is logically committed or not.
     * This information might be used by recovery procedure after unclean shutdown
     * (termination before transaction is fully committed).
     *
     * @param committed true if transaction is logically committed, false otherwise
     * @param transactionId of the corresponding transaction
     * @return undo log name
     */
    public static String getUndoLogName(boolean committed, int transactionId) {
        return UNDO_LOG_NAME_PREFIX +
                (committed ? UNDO_LOG_COMMITTED : UNDO_LOG_OPEN) +
                (transactionId > 0 ? String.valueOf(transactionId) : "");
    }

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new ObjectDataType(), 0);
    }

    /**
     * Create a new transaction store.
     *
     * @param store the store
     * @param dataType the data type for map keys and values
     * @param timeoutMillis lock acquisition timeout in milliseconds, 0 means no wait
     */
    public TransactionStore(MVStore store, DataType dataType, int timeoutMillis) {
        this.store = store;
        this.timeoutMillis = timeoutMillis;
        preparedTransactions = store.openMap("openTransactions",
                new MVMap.Builder<Integer, Object[]>());
        DataType oldValueType = new VersionedValueType(dataType);
        ArrayType undoLogValueType = new ArrayType(new DataType[]{
                new ObjectDataType(), dataType, oldValueType
        });
        undoLogBuilder = new MVMap.Builder<Long, Object[]>()
                .singleWriter()
                .valueType(undoLogValueType);
        DataType vt = new VersionedValueType(dataType);
        mapBuilder = new MVMap.Builder<Object, VersionedValue>()
                            .keyType(dataType).valueType(vt);
    }

    /**
     * Initialize the store. This is needed before a transaction can be opened.
     * If the transaction store is corrupt, this method can throw an exception,
     * in which case the store can only be used for reading.
     */
    public void init() {
        if (!init) {
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith(UNDO_LOG_NAME_PREFIX)) {
                    // Unexpectedly short name may be encountered upon upgrade from older version
                    // where undo log was persisted as a single map, remove it.
                    if (mapName.length() > UNDO_LOG_NAME_PREFIX.length()) {
                        boolean committed = mapName.charAt(UNDO_LOG_NAME_PREFIX.length()) == UNDO_LOG_COMMITTED;
                        if (store.hasData(mapName) || committed) {
                            int transactionId = StringUtils.parseUInt31(mapName, UNDO_LOG_NAME_PREFIX.length() + 1,
                                    mapName.length());
                            VersionedBitSet openTxBitSet = openTransactions.get();
                            if (!openTxBitSet.get(transactionId)) {
                                Object[] data = preparedTransactions.get(transactionId);
                                int status;
                                String name;
                                if (data == null) {
                                    status = Transaction.STATUS_OPEN;
                                    name = null;
                                } else {
                                    status = (Integer) data[0];
                                    name = (String) data[1];
                                }
                                if (committed) {
                                    status = Transaction.STATUS_COMMITTED;
                                }
                                MVMap<Long, Object[]> undoLog = store.openMap(mapName, undoLogBuilder);
                                undoLogs[transactionId] = undoLog;
                                Long lastUndoKey = undoLog.lastKey();
                                assert committed || lastUndoKey != null;
                                assert committed || getTransactionId(lastUndoKey) == transactionId;
                                long logId = lastUndoKey == null ? 0 : getLogId(lastUndoKey) + 1;
                                registerTransaction(transactionId, status, name, logId, timeoutMillis, 0,
                                        ROLLBACK_LISTENER_NONE);
                                continue;
                            }
                        }
                    }

                    if (!store.isReadOnly()) {
                        store.removeMap(mapName);
                    }
                }
            }
            init = true;
        }
    }

    /**
     * Commit all transactions that are in the committed state, and
     * rollback all open transactions.
     */
    public void endLeftoverTransactions() {
        List<Transaction> list = getOpenTransactions();
        for (Transaction t : list) {
            int status = t.getStatus();
            if (status == Transaction.STATUS_COMMITTED) {
                t.commit();
            } else if (status != Transaction.STATUS_PREPARED) {
                t.rollback();
            }
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
        DataUtils.checkArgument(max <= MAX_OPEN_TRANSACTIONS,
                "Concurrent transactions limit is too high: {0}", max);
        this.maxTransactionId = max;
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return store.hasMap(name);
    }

    private static final int LOG_ID_BITS = Transaction.LOG_ID_BITS;
    private static final long LOG_ID_MASK = (1L << LOG_ID_BITS) - 1;

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << (64 - LOG_ID_BITS)),
                "Transaction id out of range: {0}", transactionId);
        DataUtils.checkArgument(logId >= 0 && logId <= LOG_ID_MASK,
                "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << LOG_ID_BITS) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    static int getTransactionId(long operationId) {
        return (int) (operationId >>> LOG_ID_BITS);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & LOG_ID_MASK;
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    public List<Transaction> getOpenTransactions() {
        if(!init) {
            init();
        }
        ArrayList<Transaction> list = new ArrayList<>();
        int transactionId = 0;
        BitSet bitSet = openTransactions.get();
        while((transactionId = bitSet.nextSetBit(transactionId + 1)) > 0) {
            Transaction transaction = getTransaction(transactionId);
            if(transaction != null) {
                if(transaction.getStatus() != Transaction.STATUS_CLOSED) {
                    list.add(transaction);
                }
            }
        }
        return list;
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
    public Transaction begin() {
        return begin(ROLLBACK_LISTENER_NONE, timeoutMillis, 0);
    }

    /**
     * Begin a new transaction.
     * @param listener to be notified in case of a rollback
     * @param timeoutMillis to wait for a blocking transaction
     * @param ownerId of the owner (Session?) to be reported by getBlockerId
     * @return the transaction
     */
    public Transaction begin(RollbackListener listener, int timeoutMillis, int ownerId) {

        if(timeoutMillis <= 0) {
            timeoutMillis = this.timeoutMillis;
        }
        Transaction transaction = registerTransaction(0, Transaction.STATUS_OPEN, null, 0,
                timeoutMillis, ownerId, listener);
        return transaction;
    }

    private Transaction registerTransaction(int txId, int status, String name, long logId,
                                            int timeoutMillis, int ownerId, RollbackListener listener) {
        int transactionId;
        long sequenceNo;
        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            if (txId == 0) {
                transactionId = original.nextClearBit(1);
            } else {
                transactionId = txId;
                assert !original.get(transactionId);
            }
            if (transactionId > maxTransactionId) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                        "There are {0} open transactions",
                        transactionId - 1);
            }
            VersionedBitSet clone = original.clone();
            clone.set(transactionId);
            sequenceNo = clone.getVersion() + 1;
            clone.setVersion(sequenceNo);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        Transaction transaction = new Transaction(this, transactionId, sequenceNo, status, name, logId,
                timeoutMillis, ownerId, listener);

        assert transactions.get(transactionId) == null;
        transactions.set(transactionId, transaction);

        if (undoLogs[transactionId] == null) {
            String undoName = getUndoLogName(status == Transaction.STATUS_COMMITTED, transactionId);
            MVMap<Long, Object[]> undoLog = store.openMap(undoName, undoLogBuilder);
            undoLogs[transactionId] = undoLog;
        }
        return transaction;
    }

    /**
     * Store a transaction.
     *
     * @param t the transaction
     */
    void storeTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED ||
                t.getName() != null) {
            Object[] v = { t.getStatus(), t.getName() };
            preparedTransactions.put(t.getId(), v);
            t.wasStored = true;
        }
    }

    /**
     * Add an undo log entry.
     *
     * @param transactionId id of the transaction
     * @param logId sequential number of the log record within transaction
     * @param undoLogRecord Object[mapId, key, previousValue]
     * @return undo key
     */
    long addUndoLogRecord(int transactionId, long logId, Object[] undoLogRecord) {
        MVMap<Long, Object[]> undoLog = undoLogs[transactionId];
        long undoKey = getOperationId(transactionId, logId);
        if (logId == 0 && !undoLog.isEmpty()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                    "An old transaction with the same id " +
                    "is still open: {0}",
                    transactionId);
        }
        undoLog.append(undoKey, undoLogRecord);
        return undoKey;
    }

    /**
     * Remove an undo log entry.
     * @param transactionId id of the transaction
     */
    void removeUndoLogRecord(int transactionId) {
        undoLogs[transactionId].trimLast();
    }

    /**
     * Remove the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    <K, V> void removeMap(TransactionMap<K, V> map) {
        store.removeMap(map.map, false);
    }

    /**
     * Commit a transaction.
     *  @param t transaction to commit
     *  @param recovery if called during initial transaction recovery procedure
     *                  therefore undo log is stored under "committed" name already
     */
    void commit(Transaction t, boolean recovery) {
        if (!store.isClosed()) {
            int transactionId = t.transactionId;
            // this is an atomic action that causes all changes
            // made by this transaction, to be considered as "committed"
            flipCommittingTransactionsBit(transactionId, true);

            CommitDecisionMaker commitDecisionMaker = new CommitDecisionMaker();
            try {
                MVMap<Long, Object[]> undoLog = undoLogs[transactionId];
                if(!recovery) {
                    store.renameMap(undoLog, getUndoLogName(true, transactionId));
                }
                try {
                    Cursor<Long, Object[]> cursor = undoLog.cursor(null);
                    while (cursor.hasNext()) {
                        Long undoKey = cursor.next();
                        Object[] op = cursor.getValue();
                        int mapId = (Integer) op[0];
                        MVMap<Object, VersionedValue> map = openMap(mapId);
                        if (map != null) { // might be null if map was removed later
                            Object key = op[1];
                            commitDecisionMaker.setUndoKey(undoKey);
                            // although second parameter (value) is not really
                            // used by CommitDecisionMaker, MVRTreeMap has weird
                            // traversal logic based on it, and any non-null
                            // value will do, to signify update, not removal
                            map.operate(key, VersionedValue.DUMMY, commitDecisionMaker);
                        }
                    }
                    undoLog.clear();
                } finally {
                    store.renameMap(undoLog, getUndoLogName(false, transactionId));
                }
            } finally {
                flipCommittingTransactionsBit(transactionId, false);
            }
        }
    }

    private void flipCommittingTransactionsBit(int transactionId, boolean flag) {
        boolean success;
        do {
            BitSet original = committingTransactions.get();
            assert original.get(transactionId) != flag : flag ? "Double commit" : "Mysterious bit's disappearance";
            BitSet clone = (BitSet) original.clone();
            clone.set(transactionId, flag);
            success = committingTransactions.compareAndSet(original, clone);
        } while(!success);
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
    <K> MVMap<K, VersionedValue> openMap(String name,
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
        return map;
    }

    /**
     * Open the map with the given id.
     *
     * @param mapId the id
     * @return the map
     */
    MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = store.getMap(mapId);
        if (map == null) {
            String mapName = store.getMapName(mapId);
            if (mapName == null) {
                // the map was removed later on
                return null;
            }
            map = store.openMap(mapName, mapBuilder);
        }
        return map;
    }

    /**
     * End this transaction. Change status to CLOSED and vacate transaction slot.
     * Will try to commit MVStore if autocommitDelay is 0 or if database is idle
     * and amount of unsaved changes is sizable.
     *
     * @param t the transaction
     * @param hasChanges true if transaction has done any updates
     *                  (even if they are fully rolled back),
     *                   false if it just performed a data access
     */
    void endTransaction(Transaction t, boolean hasChanges) {
        t.closeIt();
        int txId = t.transactionId;
        transactions.set(txId, null);

        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            assert original.get(txId);
            VersionedBitSet clone = original.clone();
            clone.clear(txId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        if (hasChanges) {
            boolean wasStored = t.wasStored;
            if (wasStored && !preparedTransactions.isClosed()) {
                preparedTransactions.remove(txId);
            }

            if (wasStored || store.getAutoCommitDelay() == 0) {
                store.tryCommit();
            } else {
                if (isUndoEmpty()) {
                    // to avoid having to store the transaction log,
                    // if there is no open transaction,
                    // and if there have been many changes, store them now
                    int unsaved = store.getUnsavedMemory();
                    int max = store.getAutoCommitMemory();
                    // save at 3/4 capacity
                    if (unsaved * 4 > max * 3) {
                        store.tryCommit();
                    }
                }
            }
        }
    }

    private boolean isUndoEmpty() {
        BitSet openTrans = openTransactions.get();
        for (int i = openTrans.nextSetBit(0); i >= 0; i = openTrans.nextSetBit(i + 1)) {
            MVMap<Long, Object[]> undoLog = undoLogs[i];
            if (undoLog != null && !undoLog.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get Transaction object for a transaction id.
     *
     * @param transactionId id for an open transaction
     * @return Transaction object.
     */
    Transaction getTransaction(int transactionId) {
        return transactions.get(transactionId);
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        int transactionId = t.getId();
        MVMap<Long, Object[]> undoLog = undoLogs[transactionId];
        RollbackDecisionMaker decisionMaker = new RollbackDecisionMaker(this, transactionId, toLogId, t.listener);
        for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
            Long undoKey = getOperationId(transactionId, logId);
            undoLog.operate(undoKey, null, decisionMaker);
            decisionMaker.reset();
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

        final MVMap<Long, Object[]> undoLog = undoLogs[t.getId()];
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            private void fetchNext() {
                int transactionId = t.getId();
                while (logId >= toLogId) {
                    Long undoKey = getOperationId(transactionId, logId);
                    Object[] op = undoLog.get(undoKey);
                    logId--;
                    if (op == null) {
                        // partially rolled back: load previous
                        undoKey = undoLog.floorKey(undoKey);
                        if (undoKey == null || getTransactionId(undoKey) != transactionId) {
                            break;
                        }
                        logId = getLogId(undoKey);
                        continue;
                    }
                    int mapId = (int)op[0];
                    MVMap<Object, VersionedValue> m = openMap(mapId);
                    if (m != null) { // could be null if map was removed later on
                        VersionedValue oldValue = (VersionedValue) op[2];
                        current = new Change(m.getName(), op[1], oldValue == null ? null : oldValue.getCurrentValue());
                        return;
                    }
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                if(current == null) {
                    fetchNext();
                }
                return current != null;
            }

            @Override
            public Change next() {
                if(!hasNext()) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                current = null;
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
        public final String mapName;

        /**
         * The key.
         */
        public final Object key;

        /**
         * The value.
         */
        public final Object value;

        public Change(String mapName, Object key, Object value) {
            this.mapName = mapName;
            this.key = key;
            this.value = value;
        }
    }

    /**
     * This listener can be registered with the transaction to be notified of
     * every compensating change during transaction rollback.
     * Normally this is not required, if no external resources were modified,
     * because state of all transactional maps will be restored automatically.
     * Only state of external resources, possibly modified by triggers
     * need to be restored.
     */
    public interface RollbackListener {

        /**
         * Notified of a single map change (add/update/remove)
         * @param map modified
         * @param key of the modified entry
         * @param existingValue value in the map (null if delete is rolled back)
         * @param restoredValue value to be restored (null if add is rolled back)
         */
        void onRollback(MVMap<Object,VersionedValue> map, Object key,
                        VersionedValue existingValue, VersionedValue restoredValue);
    }

    private static final RollbackListener ROLLBACK_LISTENER_NONE = new RollbackListener() {
        @Override
        public void onRollback(MVMap<Object, VersionedValue> map, Object key,
                                VersionedValue existingValue, VersionedValue restoredValue) {
            // do nothing
        }
    };

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
