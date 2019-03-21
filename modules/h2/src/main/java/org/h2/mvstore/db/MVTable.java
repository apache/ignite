/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.db.MVTableEngine.Store;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.DebuggingThreadLocal;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/**
 * A table stored in a MVStore.
 */
public class MVTable extends RegularTable {
    /**
     * The table name this thread is waiting to lock.
     */
    public static final DebuggingThreadLocal<String> WAITING_FOR_LOCK;

    /**
     * The table names this thread has exclusively locked.
     */
    public static final DebuggingThreadLocal<ArrayList<String>> EXCLUSIVE_LOCKS;

    /**
     * The tables names this thread has a shared lock on.
     */
    public static final DebuggingThreadLocal<ArrayList<String>> SHARED_LOCKS;

    /**
     * The type of trace lock events
     */
    private enum TraceLockEvent{

        TRACE_LOCK_OK("ok"),
        TRACE_LOCK_WAITING_FOR("waiting for"),
        TRACE_LOCK_REQUESTING_FOR("requesting for"),
        TRACE_LOCK_TIMEOUT_AFTER("timeout after "),
        TRACE_LOCK_UNLOCK("unlock"),
        TRACE_LOCK_ADDED_FOR("added for"),
        TRACE_LOCK_ADD_UPGRADED_FOR("add (upgraded) for ");

        private final String eventText;

        TraceLockEvent(String eventText) {
            this.eventText = eventText;
        }

        public String getEventText() {
            return eventText;
        }
    }
    private static final String NO_EXTRA_INFO = "";

    static {
        if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
            WAITING_FOR_LOCK = new DebuggingThreadLocal<>();
            EXCLUSIVE_LOCKS = new DebuggingThreadLocal<>();
            SHARED_LOCKS = new DebuggingThreadLocal<>();
        } else {
            WAITING_FOR_LOCK = null;
            EXCLUSIVE_LOCKS = null;
            SHARED_LOCKS = null;
        }
    }

    private MVPrimaryIndex primaryIndex;
    private final ArrayList<Index> indexes = Utils.newSmallArrayList();
    private final AtomicLong lastModificationId = new AtomicLong();

    /**
     * The queue of sessions waiting to lock the table. It is a FIFO queue to
     * prevent starvation, since Java's synchronized locking is biased.
     */
    private final ArrayDeque<Session> waitingSessions = new ArrayDeque<>();
    private final Trace traceLock;
    private final AtomicInteger changesUntilAnalyze;
    private int nextAnalyze;

    private final MVTableEngine.Store store;
    private final TransactionStore transactionStore;

    public MVTable(CreateTableData data, MVTableEngine.Store store) {
        super(data);
        nextAnalyze = database.getSettings().analyzeAuto;
        changesUntilAnalyze = nextAnalyze <= 0 ? null : new AtomicInteger(nextAnalyze);
        this.store = store;
        this.transactionStore = store.getTransactionStore();
        traceLock = database.getTrace(Trace.LOCK);

        primaryIndex = new MVPrimaryIndex(database, this, getId(),
                IndexColumn.wrap(getColumns()), IndexType.createScan(true));
        indexes.add(primaryIndex);
    }

    public String getMapName() {
        return primaryIndex.getMapName();
    }

    @Override
    public boolean lock(Session session, boolean exclusive,
            boolean forceLockEvenInMvcc) {
        int lockMode = database.getLockMode();
        if (lockMode == Constants.LOCK_MODE_OFF) {
            return false;
        }
        if (!forceLockEvenInMvcc) {
            // MVCC: update, delete, and insert use a shared lock.
            // Select doesn't lock except when using FOR UPDATE and
            // the system property h2.selectForUpdateMvcc
            // is not enabled
            if (exclusive) {
                exclusive = false;
            } else {
                if (lockExclusiveSession == null) {
                    return false;
                }
            }
        }
        if (lockExclusiveSession == session) {
            return true;
        }
        if (!exclusive && lockSharedSessions.containsKey(session)) {
            return true;
        }
        synchronized (getLockSyncObject()) {
            if (!exclusive && lockSharedSessions.containsKey(session)) {
                return true;
            }
            session.setWaitForLock(this, Thread.currentThread());
            if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                WAITING_FOR_LOCK.set(getName());
            }
            waitingSessions.addLast(session);
            try {
                doLock1(session, lockMode, exclusive);
            } finally {
                session.setWaitForLock(null, null);
                if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                    WAITING_FOR_LOCK.remove();
                }
                waitingSessions.remove(session);
            }
        }
        return false;
    }

    /**
     * The the object on which to synchronize and wait on. For the
     * multi-threaded mode, this is this object, but for non-multi-threaded, it
     * is the database, as in this case all operations are synchronized on the
     * database object.
     *
     * @return the lock sync object
     */
    private Object getLockSyncObject() {
        if (database.isMultiThreaded()) {
            return this;
        }
        return database;
    }

    private void doLock1(Session session, int lockMode, boolean exclusive) {
        traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_REQUESTING_FOR, NO_EXTRA_INFO);
        // don't get the current time unless necessary
        long max = 0;
        boolean checkDeadlock = false;
        while (true) {
            // if I'm the next one in the queue
            if (waitingSessions.getFirst() == session) {
                if (doLock2(session, lockMode, exclusive)) {
                    return;
                }
            }
            if (checkDeadlock) {
                ArrayList<Session> sessions = checkDeadlock(session, null, null);
                if (sessions != null) {
                    throw DbException.get(ErrorCode.DEADLOCK_1,
                            getDeadlockDetails(sessions, exclusive));
                }
            } else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }
            long now = System.nanoTime();
            if (max == 0) {
                // try at least one more time
                max = now + TimeUnit.MILLISECONDS.toNanos(session.getLockTimeout());
            } else if (now >= max) {
                traceLock(session, exclusive,
                        TraceLockEvent.TRACE_LOCK_TIMEOUT_AFTER, NO_EXTRA_INFO+session.getLockTimeout());
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, getName());
            }
            try {
                traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_WAITING_FOR, NO_EXTRA_INFO);
                if (database.getLockMode() == Constants.LOCK_MODE_TABLE_GC) {
                    for (int i = 0; i < 20; i++) {
                        long free = Runtime.getRuntime().freeMemory();
                        System.gc();
                        long free2 = Runtime.getRuntime().freeMemory();
                        if (free == free2) {
                            break;
                        }
                    }
                }
                // don't wait too long so that deadlocks are detected early
                long sleep = Math.min(Constants.DEADLOCK_CHECK,
                        TimeUnit.NANOSECONDS.toMillis(max - now));
                if (sleep == 0) {
                    sleep = 1;
                }
                getLockSyncObject().wait(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private boolean doLock2(Session session, int lockMode, boolean exclusive) {
        if (lockExclusiveSession == null) {
            if (exclusive) {
                if (lockSharedSessions.isEmpty()) {
                    traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_ADDED_FOR, NO_EXTRA_INFO);
                    session.addLock(this);
                    lockExclusiveSession = session;
                    if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                        if (EXCLUSIVE_LOCKS.get() == null) {
                            EXCLUSIVE_LOCKS.set(new ArrayList<String>());
                        }
                        EXCLUSIVE_LOCKS.get().add(getName());
                    }
                    return true;
                } else if (lockSharedSessions.size() == 1 &&
                        lockSharedSessions.containsKey(session)) {
                    traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_ADD_UPGRADED_FOR, NO_EXTRA_INFO);
                    lockExclusiveSession = session;
                    if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                        if (EXCLUSIVE_LOCKS.get() == null) {
                            EXCLUSIVE_LOCKS.set(new ArrayList<String>());
                        }
                        EXCLUSIVE_LOCKS.get().add(getName());
                    }
                    return true;
                }
            } else {
                if (lockSharedSessions.putIfAbsent(session, session) == null) {
                    traceLock(session, exclusive, TraceLockEvent.TRACE_LOCK_OK, NO_EXTRA_INFO);
                    session.addLock(this);
                    if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                        ArrayList<String> list = SHARED_LOCKS.get();
                        if (list == null) {
                            list = new ArrayList<>();
                            SHARED_LOCKS.set(list);
                        }
                        list.add(getName());
                    }
                }
                return true;
            }
        }
        return false;
    }

    private void traceLock(Session session, boolean exclusive, TraceLockEvent eventEnum, String extraInfo) {
        if (traceLock.isDebugEnabled()) {
            traceLock.debug("{0} {1} {2} {3} {4}", session.getId(),
                    exclusive ? "exclusive write lock" : "shared read lock", eventEnum.getEventText(),
                    getName(), extraInfo);
        }
    }

    @Override
    public void unlock(Session s) {
        if (database != null) {
            boolean wasLocked = lockExclusiveSession == s;
            traceLock(s, wasLocked, TraceLockEvent.TRACE_LOCK_UNLOCK, NO_EXTRA_INFO);
            if (wasLocked) {
                lockSharedSessions.remove(s);
                lockExclusiveSession = null;
                if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                    if (EXCLUSIVE_LOCKS.get() != null) {
                        EXCLUSIVE_LOCKS.get().remove(getName());
                    }
                }
            } else {
                wasLocked = lockSharedSessions.remove(s) != null;
                if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
                    if (SHARED_LOCKS.get() != null) {
                        SHARED_LOCKS.get().remove(getName());
                    }
                }
            }
            if (wasLocked && !waitingSessions.isEmpty()) {
                Object lockSyncObject = getLockSyncObject();
                synchronized (lockSyncObject) {
                    lockSyncObject.notifyAll();
                }
            }
        }
    }

    @Override
    public void close(Session session) {
        // ignore
    }

    @Override
    public Row getRow(Session session, long key) {
        return primaryIndex.getRow(session, key);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
            IndexColumn[] cols, IndexType indexType, boolean create,
            String indexComment) {
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(
                            ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1,
                            column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        MVIndex index;
        int mainIndexColumn = primaryIndex.getMainIndexColumn() != SearchRow.ROWID_INDEX
                ? SearchRow.ROWID_INDEX : getMainIndexColumn(indexType, cols);
        if (database.isStarting()) {
            // if index does exists as a separate map it can't be a delegate
            if (transactionStore.hasMap("index." + indexId)) {
                // we can not reuse primary index
                mainIndexColumn = SearchRow.ROWID_INDEX;
            }
        } else if (primaryIndex.getRowCountMax() != 0) {
            mainIndexColumn = SearchRow.ROWID_INDEX;
        }

        if (mainIndexColumn != SearchRow.ROWID_INDEX) {
            primaryIndex.setMainIndexColumn(mainIndexColumn);
            index = new MVDelegateIndex(this, indexId, indexName, primaryIndex,
                    indexType);
        } else if (indexType.isSpatial()) {
            index = new MVSpatialIndex(session.getDatabase(), this, indexId,
                    indexName, cols, indexType);
        } else {
            index = new MVSecondaryIndex(session.getDatabase(), this, indexId,
                    indexName, cols, indexType);
        }
        if (index.needRebuild()) {
            rebuildIndex(session, index, indexName);
        }
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    private void rebuildIndex(Session session, MVIndex index, String indexName) {
        try {
            if (session.getDatabase().getStore() == null ||
                    index instanceof MVSpatialIndex) {
                // in-memory
                rebuildIndexBuffered(session, index);
            } else {
                rebuildIndexBlockMerge(session, index);
            }
        } catch (DbException e) {
            getSchema().freeUniqueName(indexName);
            try {
                index.remove(session);
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means
                // there is something wrong with the database
                trace.error(e2, "could not remove index");
                throw e2;
            }
            throw e;
        }
    }

    private void rebuildIndexBlockMerge(Session session, MVIndex index) {
        if (index instanceof MVSpatialIndex) {
            // the spatial index doesn't support multi-way merge sort
            rebuildIndexBuffered(session, index);
        }
        // Read entries in memory, sort them, write to a new map (in sorted
        // order); repeat (using a new map for every block of 1 MB) until all
        // record are read. Merge all maps to the target (using merge sort;
        // duplicates are detected in the target). For randomly ordered data,
        // this should use relatively few write operations.
        // A possible optimization is: change the buffer size from "row count"
        // to "amount of memory", and buffer index keys instead of rows.
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        Store store = session.getDatabase().getStore();

        int bufferSize = database.getMaxMemoryRows() / 2;
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        ArrayList<String> bufferNames = Utils.newSmallArrayList();
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                    MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                sortRows(buffer, index);
                String mapName = store.nextTemporaryMapName();
                index.addRowsToBuffer(buffer, mapName);
                bufferNames.add(mapName);
                buffer.clear();
            }
            remaining--;
        }
        sortRows(buffer, index);
        if (!bufferNames.isEmpty()) {
            String mapName = store.nextTemporaryMapName();
            index.addRowsToBuffer(buffer, mapName);
            bufferNames.add(mapName);
            buffer.clear();
            index.addBufferedRows(bufferNames);
        } else {
            addRowsToIndex(session, buffer, index);
        }
        if (remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining +
                    " " + getName());
        }
    }

    private void rebuildIndexBuffered(Session session, Index index) {
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;
        int bufferSize = (int) Math.min(total, database.getMaxMemoryRows());
        ArrayList<Row> buffer = new ArrayList<>(bufferSize);
        String n = getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        while (cursor.next()) {
            Row row = cursor.get();
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                    MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                addRowsToIndex(session, buffer, index);
            }
            remaining--;
        }
        addRowsToIndex(session, buffer, index);
        if (remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining +
                    " " + getName());
        }
    }

    @Override
    public void removeRow(Session session, Row row) {
        syncLastModificationIdWithDatabase();
        Transaction t = session.getTransaction();
        long savepoint = t.setSavepoint();
        try {
            for (int i = indexes.size() - 1; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
            }
        } catch (Throwable e) {
            try {
                t.rollbackToSavepoint(savepoint);
            } catch (Throwable nested) {
                e.addSuppressed(nested);
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public void truncate(Session session) {
        syncLastModificationIdWithDatabase();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        if (changesUntilAnalyze != null) {
            changesUntilAnalyze.set(nextAnalyze);
        }
    }

    @Override
    public void addRow(Session session, Row row) {
        syncLastModificationIdWithDatabase();
        Transaction t = session.getTransaction();
        long savepoint = t.setSavepoint();
        try {
            for (Index index : indexes) {
                index.add(session, row);
            }
        } catch (Throwable e) {
            try {
                t.rollbackToSavepoint(savepoint);
            } catch (Throwable nested) {
                e.addSuppressed(nested);
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public void updateRow(Session session, Row oldRow, Row newRow) {
        newRow.setKey(oldRow.getKey());
        syncLastModificationIdWithDatabase();
        Transaction t = session.getTransaction();
        long savepoint = t.setSavepoint();
        try {
            for (Index index : indexes) {
                index.update(session, oldRow, newRow);
            }
        } catch (Throwable e) {
            try {
                t.rollbackToSavepoint(savepoint);
            } catch (Throwable nested) {
                e.addSuppressed(nested);
            }
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public Row lockRow(Session session, Row row) {
        return primaryIndex.lockRow(session, row);
    }

    private void analyzeIfRequired(Session session) {
        if (changesUntilAnalyze != null) {
            if (changesUntilAnalyze.decrementAndGet() == 0) {
                if (nextAnalyze <= Integer.MAX_VALUE / 2) {
                    nextAnalyze *= 2;
                }
                changesUntilAnalyze.set(nextAnalyze);
                session.markTableForAnalyze(this);
            }
        }
    }

    @Override
    public Index getScanIndex(Session session) {
        return primaryIndex;
    }

    @Override
    public Index getUniqueIndex() {
        return primaryIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId.get();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        if (containsLargeObject) {
            // unfortunately, the data is gone on rollback
            truncate(session);
            database.getLobStorage().removeAllForTable(getId());
            database.lockMeta(session);
        }
        database.getStore().removeTable(this);
        super.removeChildrenAndResources(session);
        // remove scan index (at position 0 on the list) last
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            index.remove(session);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
            // needed for session temporary indexes
            indexes.remove(index);
        }
        primaryIndex.remove(session);
        indexes.clear();
        if (SysProperties.CHECK) {
            for (SchemaObject obj : database
                    .getAllSchemaObjects(DbObject.INDEX)) {
                Index index = (Index) obj;
                if (index.getTable() == this) {
                    DbException.throwInternalError("index not dropped: " +
                            index.getName());
                }
            }
        }
        close(session);
        invalidate();
    }

    @Override
    public long getRowCount(Session session) {
        return primaryIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return primaryIndex.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return primaryIndex.getDiskSpaceUsed();
    }

    /**
     * Get a new transaction.
     *
     * @return the transaction
     */
    Transaction getTransactionBegin() {
        // TODO need to commit/rollback the transaction
        return transactionStore.begin();
    }

    @Override
    public boolean isMVStore() {
        return true;
    }

    /**
     * Mark the transaction as committed, so that the modification counter of
     * the database is incremented.
     */
    public void commit() {
        if (database != null) {
            syncLastModificationIdWithDatabase();
        }
    }

    // Field lastModificationId can not be just a volatile, because window of opportunity
    // between reading database's modification id and storing this value in the field
    // could be exploited by another thread.
    // Second thread may do the same with possibly bigger (already advanced)
    // modification id, and when first thread finally updates the field, it will
    // result in lastModificationId jumping back.
    // This is, of course, unacceptable.
    private void syncLastModificationIdWithDatabase() {
        long nextModificationDataId = database.getNextModificationDataId();
        long currentId;
        do {
            currentId = lastModificationId.get();
        } while (nextModificationDataId > currentId &&
                !lastModificationId.compareAndSet(currentId, nextModificationDataId));
    }

    /**
     * Convert the illegal state exception to a database exception.
     *
     * @param e the illegal state exception
     * @return the database exception
     */
    DbException convertException(IllegalStateException e) {
        int errorCode = DataUtils.getErrorCode(e.getMessage());
        if (errorCode == DataUtils.ERROR_TRANSACTION_LOCKED) {
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1,
                    e, getName());
        }
        if (errorCode == DataUtils.ERROR_TRANSACTIONS_DEADLOCK) {
            throw DbException.get(ErrorCode.DEADLOCK_1,
                    e, getName());
        }
        return store.convertIllegalStateException(e);
    }
}
