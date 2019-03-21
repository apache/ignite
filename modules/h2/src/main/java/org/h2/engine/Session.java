/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.api.ErrorCode;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.ddl.Analyze;
import org.h2.command.dml.Query;
import org.h2.constraint.Constraint;
import org.h2.index.Index;
import org.h2.index.ViewIndex;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.db.MVTable;
import org.h2.mvstore.db.MVTableEngine;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.value.VersionedValue;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.store.DataHandler;
import org.h2.store.InDoubtTransaction;
import org.h2.store.LobStorageFrontend;
import org.h2.table.SubQueryInfo;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableType;
import org.h2.util.ColumnNamerConfiguration;
import org.h2.util.CurrentTimestamp;
import org.h2.util.SmallLRUCache;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTimestampTimeZone;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public class Session extends SessionWithState implements TransactionStore.RollbackListener {

    public enum State { INIT, RUNNING, BLOCKED, SLEEP, CLOSED }

    /**
     * This special log position means that the log entry has been written.
     */
    public static final int LOG_WRITTEN = -1;

    /**
     * The prefix of generated identifiers. It may not have letters, because
     * they are case sensitive.
     */
    private static final String SYSTEM_IDENTIFIER_PREFIX = "_";
    private static int nextSerialId;

    private final int serialId = nextSerialId++;
    private final Database database;
    private ConnectionInfo connectionInfo;
    private final User user;
    private final int id;
    private final ArrayList<Table> locks = Utils.newSmallArrayList();
    private UndoLog undoLog;
    private boolean autoCommit = true;
    private Random random;
    private int lockTimeout;
    private Value lastIdentity = ValueLong.get(0);
    private Value lastScopeIdentity = ValueLong.get(0);
    private Value lastTriggerIdentity;
    private GeneratedKeys generatedKeys;
    private int firstUncommittedLog = Session.LOG_WRITTEN;
    private int firstUncommittedPos = Session.LOG_WRITTEN;
    private HashMap<String, Savepoint> savepoints;
    private HashMap<String, Table> localTempTables;
    private HashMap<String, Index> localTempTableIndexes;
    private HashMap<String, Constraint> localTempTableConstraints;
    private long throttleNs;
    private long lastThrottle;
    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String[] schemaSearchPath;
    private Trace trace;
    private HashMap<String, Value> removeLobMap;
    private int systemIdentifier;
    private HashMap<String, Procedure> procedures;
    private boolean undoLogEnabled = true;
    private boolean redoLogBinary = true;
    private boolean autoCommitAtTransactionEnd;
    private String currentTransactionName;
    private volatile long cancelAtNs;
    private final long sessionStart = System.currentTimeMillis();
    private ValueTimestampTimeZone transactionStart;
    private ValueTimestampTimeZone currentCommandStart;
    private HashMap<String, Value> variables;
    private HashSet<ResultInterface> temporaryResults;
    private int queryTimeout;
    private boolean commitOrRollbackDisabled;
    private Table waitForLock;
    private Thread waitForLockThread;
    private int modificationId;
    private int objectId;
    private final int queryCacheSize;
    private SmallLRUCache<String, Command> queryCache;
    private long modificationMetaID = -1;
    private SubQueryInfo subQueryInfo;
    private ArrayDeque<String> viewNameStack;
    private int preparingQueryExpression;
    private volatile SmallLRUCache<Object, ViewIndex> viewIndexCache;
    private HashMap<Object, ViewIndex> subQueryIndexCache;
    private boolean joinBatchEnabled;
    private boolean forceJoinOrder;
    private boolean lazyQueryExecution;
    private ColumnNamerConfiguration columnNamerConfiguration;
    /**
     * Tables marked for ANALYZE after the current transaction is committed.
     * Prevents us calling ANALYZE repeatedly in large transactions.
     */
    private HashSet<Table> tablesToAnalyze;

    /**
     * Temporary LOBs from result sets. Those are kept for some time. The
     * problem is that transactions are committed before the result is returned,
     * and in some cases the next transaction is already started before the
     * result is read (for example when using the server mode, when accessing
     * metadata methods). We can't simply free those values up when starting the
     * next transaction, because they would be removed too early.
     */
    private LinkedList<TimeoutValue> temporaryResultLobs;

    /**
     * The temporary LOBs that need to be removed on commit.
     */
    private ArrayList<Value> temporaryLobs;

    private Transaction transaction;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private long startStatement = -1;

    /**
     * Set of database object ids to be released at the end of transaction
     */
    private BitSet idsToRelease;

    public Session(Database database, User user, int id) {
        this.database = database;
        this.queryTimeout = database.getSettings().maxQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.user = user;
        this.id = id;
        this.lockTimeout = database.getLockTimeout();
        // PageStore creates a system session before initialization of the main schema
        Schema mainSchema = database.getMainSchema();
        this.currentSchemaName = mainSchema != null ? mainSchema.getName()
                : database.sysIdentifier(Constants.SCHEMA_MAIN);
        this.columnNamerConfiguration = ColumnNamerConfiguration.getDefault();
    }

    public void setLazyQueryExecution(boolean lazyQueryExecution) {
        this.lazyQueryExecution = lazyQueryExecution;
    }

    public boolean isLazyQueryExecution() {
        return lazyQueryExecution;
    }

    public void setForceJoinOrder(boolean forceJoinOrder) {
        this.forceJoinOrder = forceJoinOrder;
    }

    public boolean isForceJoinOrder() {
        return forceJoinOrder;
    }

    public void setJoinBatchEnabled(boolean joinBatchEnabled) {
        this.joinBatchEnabled = joinBatchEnabled;
    }

    public boolean isJoinBatchEnabled() {
        return joinBatchEnabled;
    }

    /**
     * Create a new row for a table.
     *
     * @param data the values
     * @param memory whether the row is in memory
     * @return the created row
     */
    public Row createRow(Value[] data, int memory) {
        return database.createRow(data, memory);
    }

    /**
     * Add a subquery info on top of the subquery info stack.
     *
     * @param masks the mask
     * @param filters the filters
     * @param filter the filter index
     * @param sortOrder the sort order
     */
    public void pushSubQueryInfo(int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder) {
        subQueryInfo = new SubQueryInfo(subQueryInfo, masks, filters, filter, sortOrder);
    }

    /**
     * Remove the current subquery info from the stack.
     */
    public void popSubQueryInfo() {
        subQueryInfo = subQueryInfo.getUpper();
    }

    public SubQueryInfo getSubQueryInfo() {
        return subQueryInfo;
    }

    /**
     * Stores name of currently parsed view in a stack so it can be determined
     * during {@code prepare()}.
     *
     * @param parsingView
     *            {@code true} to store one more name, {@code false} to remove it
     *            from stack
     * @param viewName
     *            name of the view
     */
    public void setParsingCreateView(boolean parsingView, String viewName) {
        if (viewNameStack == null) {
            viewNameStack = new ArrayDeque<>(3);
        }
        if (parsingView) {
            viewNameStack.push(viewName);
        } else {
            String name = viewNameStack.pop();
            assert viewName.equals(name);
        }
    }

    public String getParsingCreateViewName() {
        return viewNameStack != null ? viewNameStack.peek() : null;
    }

    public boolean isParsingCreateView() {
        return viewNameStack != null && !viewNameStack.isEmpty();
    }

    /**
     * Optimize a query. This will remember the subquery info, clear it, prepare
     * the query, and reset the subquery info.
     *
     * @param query the query to prepare
     */
    public void optimizeQueryExpression(Query query) {
        // we have to hide current subQueryInfo if we are going to optimize
        // query expression
        SubQueryInfo tmp = subQueryInfo;
        subQueryInfo = null;
        preparingQueryExpression++;
        try {
            query.prepare();
        } finally {
            subQueryInfo = tmp;
            preparingQueryExpression--;
        }
    }

    public boolean isPreparingQueryExpression() {
        assert preparingQueryExpression >= 0;
        return preparingQueryExpression != 0;
    }

    @Override
    public ArrayList<String> getClusterServers() {
        return new ArrayList<>();
    }

    public boolean setCommitOrRollbackDisabled(boolean x) {
        boolean old = commitOrRollbackDisabled;
        commitOrRollbackDisabled = x;
        return old;
    }

    private void initVariables() {
        if (variables == null) {
            variables = database.newStringMap();
        }
    }

    /**
     * Set the value of the given variable for this session.
     *
     * @param name the name of the variable (may not be null)
     * @param value the new value (may not be null)
     */
    public void setVariable(String name, Value value) {
        initVariables();
        modificationId++;
        Value old;
        if (value == ValueNull.INSTANCE) {
            old = variables.remove(name);
        } else {
            // link LOB values, to make sure we have our own object
            value = value.copy(database,
                    LobStorageFrontend.TABLE_ID_SESSION_VARIABLE);
            old = variables.put(name, value);
        }
        if (old != null) {
            // remove the old value (in case it is a lob)
            old.remove();
        }
    }

    /**
     * Get the value of the specified user defined variable. This method always
     * returns a value; it returns ValueNull.INSTANCE if the variable doesn't
     * exist.
     *
     * @param name the variable name
     * @return the value, or NULL
     */
    public Value getVariable(String name) {
        initVariables();
        Value v = variables.get(name);
        return v == null ? ValueNull.INSTANCE : v;
    }

    /**
     * Get the list of variable names that are set for this session.
     *
     * @return the list of names
     */
    public String[] getVariableNames() {
        if (variables == null) {
            return new String[0];
        }
        return variables.keySet().toArray(new String[variables.size()]);
    }

    /**
     * Get the local temporary table if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Table findLocalTempTable(String name) {
        if (localTempTables == null) {
            return null;
        }
        return localTempTables.get(name);
    }

    public ArrayList<Table> getLocalTempTables() {
        if (localTempTables == null) {
            return Utils.newSmallArrayList();
        }
        return new ArrayList<>(localTempTables.values());
    }

    /**
     * Add a local temporary table to this session.
     *
     * @param table the table to add
     * @throws DbException if a table with this name already exists
     */
    public void addLocalTempTable(Table table) {
        if (localTempTables == null) {
            localTempTables = database.newStringMap();
        }
        if (localTempTables.get(table.getName()) != null) {
            StringBuilder builder = new StringBuilder();
            table.getSQL(builder, false).append(" AS ");
            Parser.quoteIdentifier(table.getName(), false);
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, builder.toString());
        }
        modificationId++;
        localTempTables.put(table.getName(), table);
    }

    /**
     * Drop and remove the given local temporary table from this session.
     *
     * @param table the table
     */
    public void removeLocalTempTable(Table table) {
        // Exception thrown in org.h2.engine.Database.removeMeta if line below
        // is missing with TestGeneralCommonTableQueries
        boolean wasLocked = database.lockMeta(this);
        try {
            modificationId++;
            if (localTempTables != null) {
                localTempTables.remove(table.getName());
            }
            synchronized (database) {
                table.removeChildrenAndResources(this);
            }
        } finally {
            if (!wasLocked) {
                database.unlockMeta(this);
            }
        }
    }

    /**
     * Get the local temporary index if one exists with that name, or null if
     * not.
     *
     * @param name the table name
     * @return the table, or null
     */
    public Index findLocalTempTableIndex(String name) {
        if (localTempTableIndexes == null) {
            return null;
        }
        return localTempTableIndexes.get(name);
    }

    public HashMap<String, Index> getLocalTempTableIndexes() {
        if (localTempTableIndexes == null) {
            return new HashMap<>();
        }
        return localTempTableIndexes;
    }

    /**
     * Add a local temporary index to this session.
     *
     * @param index the index to add
     * @throws DbException if a index with this name already exists
     */
    public void addLocalTempTableIndex(Index index) {
        if (localTempTableIndexes == null) {
            localTempTableIndexes = database.newStringMap();
        }
        if (localTempTableIndexes.get(index.getName()) != null) {
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, index.getSQL(false));
        }
        localTempTableIndexes.put(index.getName(), index);
    }

    /**
     * Drop and remove the given local temporary index from this session.
     *
     * @param index the index
     */
    public void removeLocalTempTableIndex(Index index) {
        if (localTempTableIndexes != null) {
            localTempTableIndexes.remove(index.getName());
            synchronized (database) {
                index.removeChildrenAndResources(this);
            }
        }
    }

    /**
     * Get the local temporary constraint if one exists with that name, or
     * null if not.
     *
     * @param name the constraint name
     * @return the constraint, or null
     */
    public Constraint findLocalTempTableConstraint(String name) {
        if (localTempTableConstraints == null) {
            return null;
        }
        return localTempTableConstraints.get(name);
    }

    /**
     * Get the map of constraints for all constraints on local, temporary
     * tables, if any. The map's keys are the constraints' names.
     *
     * @return the map of constraints, or null
     */
    public HashMap<String, Constraint> getLocalTempTableConstraints() {
        if (localTempTableConstraints == null) {
            return new HashMap<>();
        }
        return localTempTableConstraints;
    }

    /**
     * Add a local temporary constraint to this session.
     *
     * @param constraint the constraint to add
     * @throws DbException if a constraint with the same name already exists
     */
    public void addLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints == null) {
            localTempTableConstraints = database.newStringMap();
        }
        String name = constraint.getName();
        if (localTempTableConstraints.get(name) != null) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraint.getSQL(false));
        }
        localTempTableConstraints.put(name, constraint);
    }

    /**
     * Drop and remove the given local temporary constraint from this session.
     *
     * @param constraint the constraint
     */
    void removeLocalTempTableConstraint(Constraint constraint) {
        if (localTempTableConstraints != null) {
            localTempTableConstraints.remove(constraint.getName());
            synchronized (database) {
                constraint.removeChildrenAndResources(this);
            }
        }
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    public User getUser() {
        return user;
    }

    @Override
    public void setAutoCommit(boolean b) {
        autoCommit = b;
    }

    public int getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    @Override
    public synchronized CommandInterface prepareCommand(String sql,
            int fetchSize) {
        return prepareLocal(sql);
    }

    /**
     * Parse and prepare the given SQL statement. This method also checks the
     * rights.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Prepared prepare(String sql) {
        return prepare(sql, false, false);
    }

    /**
     * Parse and prepare the given SQL statement.
     *
     * @param sql the SQL statement
     * @param rightsChecked true if the rights have already been checked
     * @param literalsChecked true if the sql string has already been checked
     *            for literals (only used if ALLOW_LITERALS NONE is set).
     * @return the prepared statement
     */
    public Prepared prepare(String sql, boolean rightsChecked, boolean literalsChecked) {
        Parser parser = new Parser(this);
        parser.setRightsChecked(rightsChecked);
        parser.setLiteralsChecked(literalsChecked);
        return parser.prepare(sql);
    }

    /**
     * Parse and prepare the given SQL statement.
     * This method also checks if the connection has been closed.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Command prepareLocal(String sql) {
        if (isClosed()) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "session closed");
        }
        Command command;
        if (queryCacheSize > 0) {
            if (queryCache == null) {
                queryCache = SmallLRUCache.newInstance(queryCacheSize);
                modificationMetaID = database.getModificationMetaId();
            } else {
                long newModificationMetaID = database.getModificationMetaId();
                if (newModificationMetaID != modificationMetaID) {
                    queryCache.clear();
                    modificationMetaID = newModificationMetaID;
                }
                command = queryCache.get(sql);
                if (command != null && command.canReuse()) {
                    command.reuse();
                    return command;
                }
            }
        }
        Parser parser = new Parser(this);
        try {
            command = parser.prepareCommand(sql);
        } finally {
            // we can't reuse sub-query indexes, so just drop the whole cache
            subQueryIndexCache = null;
        }
        command.prepareJoinBatch();
        if (queryCache != null) {
            if (command.isCacheable()) {
                queryCache.put(sql, command);
            }
        }
        return command;
    }

    /**
     * Arranges for the specified database object id to be released
     * at the end of the current transaction.
     * @param id to be scheduled
     */
    void scheduleDatabaseObjectIdForRelease(int id) {
        if (idsToRelease == null) {
            idsToRelease = new BitSet();
        }
        idsToRelease.set(id);
    }

    public Database getDatabase() {
        return database;
    }

    @Override
    public int getPowerOffCount() {
        return database.getPowerOffCount();
    }

    @Override
    public void setPowerOffCount(int count) {
        database.setPowerOffCount(count);
    }

    /**
     * Commit the current transaction. If the statement was not a data
     * definition statement, and if there are temporary tables that should be
     * dropped or truncated at commit, this is done as well.
     *
     * @param ddl if the statement was a data definition statement
     */
    public void commit(boolean ddl) {
        checkCommitRollback();

        currentTransactionName = null;
        transactionStart = null;
        if (transaction != null) {
            try {
                // increment the data mod count, so that other sessions
                // see the changes
                // TODO should not rely on locking
                if (!locks.isEmpty()) {
                    for (Table t : locks) {
                        if (t instanceof MVTable) {
                            ((MVTable) t).commit();
                        }
                    }
                }
                transaction.commit();
            } finally {
                transaction = null;
            }
        } else if (containsUncommitted()) {
            // need to commit even if rollback is not possible
            // (create/drop table and so on)
            database.commit(this);
        }
        removeTemporaryLobs(true);
        if (undoLog != null && undoLog.size() > 0) {
            undoLog.clear();
        }
        if (!ddl) {
            // do not clean the temp tables if the last command was a
            // create/drop
            cleanTempTables(false);
            if (autoCommitAtTransactionEnd) {
                autoCommit = true;
                autoCommitAtTransactionEnd = false;
            }
        }

        if (tablesToAnalyze != null) {
            if (database.isMVStore()) {
                // table analysis will cause a new transaction(s) to be opened,
                // so we need to commit afterwards whatever leftovers might be
                analyzeTables();
                commit(true);
            } else {
                analyzeTables();
            }
        }
        endTransaction();
    }

    private void analyzeTables() {
        int rowCount = getDatabase().getSettings().analyzeSample / 10;
        for (Table table : tablesToAnalyze) {
            Analyze.analyzeTable(this, table, rowCount, false);
        }
        // analyze can lock the meta
        database.unlockMeta(this);
        tablesToAnalyze = null;
    }

    private void removeTemporaryLobs(boolean onTimeout) {
        assert this != getDatabase().getLobSession() || Thread.holdsLock(this) || Thread.holdsLock(getDatabase());
        if (temporaryLobs != null) {
            for (Value v : temporaryLobs) {
                if (!v.isLinkedToTable()) {
                    v.remove();
                }
            }
            temporaryLobs.clear();
        }
        if (temporaryResultLobs != null && !temporaryResultLobs.isEmpty()) {
            long keepYoungerThan = System.nanoTime() -
                    TimeUnit.MILLISECONDS.toNanos(database.getSettings().lobTimeout);
            while (!temporaryResultLobs.isEmpty()) {
                TimeoutValue tv = temporaryResultLobs.getFirst();
                if (onTimeout && tv.created >= keepYoungerThan) {
                    break;
                }
                Value v = temporaryResultLobs.removeFirst().value;
                if (!v.isLinkedToTable()) {
                    v.remove();
                }
            }
        }
    }

    private void checkCommitRollback() {
        if (commitOrRollbackDisabled && !locks.isEmpty()) {
            throw DbException.get(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED);
        }
    }

    private void endTransaction() {
        if (removeLobMap != null && removeLobMap.size() > 0) {
            if (database.getStore() == null) {
                // need to flush the transaction log, because we can't unlink
                // lobs if the commit record is not written
                database.flush();
            }
            for (Value v : removeLobMap.values()) {
                v.remove();
            }
            removeLobMap = null;
        }
        unlockAll();
        if (idsToRelease != null) {
            database.releaseDatabaseObjectIds(idsToRelease);
            idsToRelease = null;
        }
    }

    /**
     * Fully roll back the current transaction.
     */
    public void rollback() {
        checkCommitRollback();
        currentTransactionName = null;
        transactionStart = null;
        boolean needCommit = undoLog != null && undoLog.size() > 0 || transaction != null;
        if (needCommit) {
            rollbackTo(null);
        }
        if (!locks.isEmpty() || needCommit) {
            database.commit(this);
        }
        idsToRelease = null;
        cleanTempTables(false);
        if (autoCommitAtTransactionEnd) {
            autoCommit = true;
            autoCommitAtTransactionEnd = false;
        }
        endTransaction();
    }

    /**
     * Partially roll back the current transaction.
     *
     * @param savepoint the savepoint to which should be rolled back
     */
    public void rollbackTo(Savepoint savepoint) {
        int index = savepoint == null ? 0 : savepoint.logIndex;
        if (undoLog != null) {
            while (undoLog.size() > index) {
                UndoLogRecord entry = undoLog.getLast();
                entry.undo(this);
                undoLog.removeLast();
            }
        }
        if (transaction != null) {
            if (savepoint == null) {
                transaction.rollback();
                transaction = null;
            } else {
                transaction.rollbackToSavepoint(savepoint.transactionSavepoint);
            }
        }
        if (savepoints != null) {
            String[] names = savepoints.keySet().toArray(new String[savepoints.size()]);
            for (String name : names) {
                Savepoint sp = savepoints.get(name);
                int savepointIndex = sp.logIndex;
                if (savepointIndex > index) {
                    savepoints.remove(name);
                }
            }
        }

        // Because cache may have captured query result (in Query.lastResult),
        // which is based on data from uncommitted transaction.,
        // It is not valid after rollback, therefore cache has to be cleared.
        if(queryCache != null) {
            queryCache.clear();
        }
    }

    @Override
    public boolean hasPendingTransaction() {
        return undoLog != null && undoLog.size() > 0;
    }

    /**
     * Create a savepoint to allow rolling back to this state.
     *
     * @return the savepoint
     */
    public Savepoint setSavepoint() {
        Savepoint sp = new Savepoint();
        if (undoLog != null) {
            sp.logIndex = undoLog.size();
        }
        if (database.getStore() != null) {
            sp.transactionSavepoint = getStatementSavepoint();
        }
        return sp;
    }

    public int getId() {
        return id;
    }

    @Override
    public void cancel() {
        cancelAtNs = System.nanoTime();
    }

    @Override
    public void close() {
        // this is the only operation that can be invoked concurrently
        // so, we should prevent double-closure
        if (state.getAndSet(State.CLOSED) != State.CLOSED) {
            try {
                database.checkPowerOff();

                // release any open table locks
                rollback();

                removeTemporaryLobs(false);
                cleanTempTables(true);
                commit(true);       // temp table removal may have opened new transaction
                if (undoLog != null) {
                    undoLog.clear();
                }
                // Table#removeChildrenAndResources can take the meta lock,
                // and we need to unlock before we call removeSession(), which might
                // want to take the meta lock using the system session.
                database.unlockMeta(this);
            } finally {
                database.removeSession(this);
            }
        }
    }

    /**
     * Add a lock for the given table. The object is unlocked on commit or
     * rollback.
     *
     * @param table the table that is locked
     */
    public void addLock(Table table) {
        if (SysProperties.CHECK) {
            if (locks.contains(table)) {
                DbException.throwInternalError(table.toString());
            }
        }
        locks.add(table);
    }

    /**
     * Add an undo log entry to this session.
     *
     * @param table the table
     * @param operation the operation type (see {@link UndoLogRecord})
     * @param row the row
     */
    public void log(Table table, short operation, Row row) {
        if (table.isMVStore()) {
            return;
        }
        if (undoLogEnabled) {
            UndoLogRecord log = new UndoLogRecord(table, operation, row);
            // called _after_ the row was inserted successfully into the table,
            // otherwise rollback will try to rollback a not-inserted row
            if (SysProperties.CHECK) {
                int lockMode = database.getLockMode();
                if (lockMode != Constants.LOCK_MODE_OFF &&
                        !database.isMVStore()) {
                    TableType tableType = log.getTable().getTableType();
                    if (!locks.contains(log.getTable())
                            && TableType.TABLE_LINK != tableType
                            && TableType.EXTERNAL_TABLE_ENGINE != tableType) {
                        DbException.throwInternalError(String.valueOf(tableType));
                    }
                }
            }
            if (undoLog == null) {
                undoLog = new UndoLog(database);
            }
            undoLog.add(log);
        }
    }

    /**
     * Unlock all read locks. This is done if the transaction isolation mode is
     * READ_COMMITTED.
     */
    public void unlockReadLocks() {
        if (!database.isMVStore() && database.isMultiThreaded() &&
                database.getLockMode() == Constants.LOCK_MODE_READ_COMMITTED) {
            for (Iterator<Table> iter = locks.iterator(); iter.hasNext(); ) {
                Table t = iter.next();
                if (!t.isLockedExclusively()) {
                    t.unlock(this);
                    iter.remove();
                }
            }
        }
    }

    /**
     * Unlock just this table.
     *
     * @param t the table to unlock
     */
    void unlock(Table t) {
        locks.remove(t);
    }

    private void unlockAll() {
        if (undoLog != null && undoLog.size() > 0) {
            DbException.throwInternalError();
        }
        if (!locks.isEmpty()) {
            for (Table t : locks) {
                t.unlock(this);
            }
            locks.clear();
        }
        database.unlockMetaDebug(this);
        savepoints = null;
        sessionStateChanged = true;
    }

    private void cleanTempTables(boolean closeSession) {
        if (localTempTables != null && localTempTables.size() > 0) {
            if (database.isMVStore()) {
                _cleanTempTables(closeSession);
            } else {
                synchronized (database) {
                    _cleanTempTables(closeSession);
                }
            }
        }
    }

    private void _cleanTempTables(boolean closeSession) {
        Iterator<Table> it = localTempTables.values().iterator();
        while (it.hasNext()) {
            Table table = it.next();
            if (closeSession || table.getOnCommitDrop()) {
                modificationId++;
                table.setModified();
                it.remove();
                // Exception thrown in org.h2.engine.Database.removeMeta
                // if line below is missing with TestDeadlock
                database.lockMeta(this);
                table.removeChildrenAndResources(this);
                if (closeSession) {
                    // need to commit, otherwise recovery might
                    // ignore the table removal
                    database.commit(this);
                }
            } else if (table.getOnCommitTruncate()) {
                table.truncate(this);
            }
        }
    }

    public Random getRandom() {
        if (random == null) {
            random = new Random();
        }
        return random;
    }

    @Override
    public Trace getTrace() {
        if (trace != null && !isClosed()) {
            return trace;
        }
        String traceModuleName = "jdbc[" + id + "]";
        if (isClosed()) {
            return new TraceSystem(null).getTrace(traceModuleName);
        }
        trace = database.getTraceSystem().getTrace(traceModuleName);
        return trace;
    }

    public void setLastIdentity(Value last) {
        this.lastIdentity = last;
        this.lastScopeIdentity = last;
    }

    public Value getLastIdentity() {
        return lastIdentity;
    }

    public void setLastScopeIdentity(Value last) {
        this.lastScopeIdentity = last;
    }

    public Value getLastScopeIdentity() {
        return lastScopeIdentity;
    }

    public void setLastTriggerIdentity(Value last) {
        this.lastTriggerIdentity = last;
    }

    public Value getLastTriggerIdentity() {
        return lastTriggerIdentity;
    }

    public GeneratedKeys getGeneratedKeys() {
        if (generatedKeys == null) {
            generatedKeys = new GeneratedKeys();
        }
        return generatedKeys;
    }

    /**
     * Called when a log entry for this session is added. The session keeps
     * track of the first entry in the transaction log that is not yet
     * committed.
     *
     * @param logId the transaction log id
     * @param pos the position of the log entry in the transaction log
     */
    public void addLogPos(int logId, int pos) {
        if (firstUncommittedLog == Session.LOG_WRITTEN) {
            firstUncommittedLog = logId;
            firstUncommittedPos = pos;
        }
    }

    public int getFirstUncommittedLog() {
        return firstUncommittedLog;
    }

    /**
     * This method is called after the transaction log has written the commit
     * entry for this session.
     */
    void setAllCommitted() {
        firstUncommittedLog = Session.LOG_WRITTEN;
        firstUncommittedPos = Session.LOG_WRITTEN;
    }

    /**
     * Whether the session contains any uncommitted changes.
     *
     * @return true if yes
     */
    public boolean containsUncommitted() {
        if (database.getStore() != null) {
            return transaction != null && transaction.hasChanges();
        }
        return firstUncommittedLog != Session.LOG_WRITTEN;
    }

    /**
     * Create a savepoint that is linked to the current log position.
     *
     * @param name the savepoint name
     */
    public void addSavepoint(String name) {
        if (savepoints == null) {
            savepoints = database.newStringMap();
        }
        savepoints.put(name, setSavepoint());
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) {
        checkCommitRollback();
        currentTransactionName = null;
        transactionStart = null;
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        Savepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        rollbackTo(savepoint);
    }

    /**
     * Prepare the given transaction.
     *
     * @param transactionName the name of the transaction
     */
    public void prepareCommit(String transactionName) {
        if (containsUncommitted()) {
            // need to commit even if rollback is not possible (create/drop
            // table and so on)
            database.prepareCommit(this, transactionName);
        }
        currentTransactionName = transactionName;
    }

    /**
     * Commit or roll back the given transaction.
     *
     * @param transactionName the name of the transaction
     * @param commit true for commit, false for rollback
     */
    public void setPreparedTransaction(String transactionName, boolean commit) {
        if (currentTransactionName != null &&
                currentTransactionName.equals(transactionName)) {
            if (commit) {
                commit(false);
            } else {
                rollback();
            }
        } else {
            ArrayList<InDoubtTransaction> list = database
                    .getInDoubtTransactions();
            int state = commit ? InDoubtTransaction.COMMIT
                    : InDoubtTransaction.ROLLBACK;
            boolean found = false;
            if (list != null) {
                for (InDoubtTransaction p: list) {
                    if (p.getTransactionName().equals(transactionName)) {
                        p.setState(state);
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                throw DbException.get(ErrorCode.TRANSACTION_NOT_FOUND_1,
                        transactionName);
            }
        }
    }

    @Override
    public boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    public void setThrottle(int throttle) {
        this.throttleNs = TimeUnit.MILLISECONDS.toNanos(throttle);
    }

    /**
     * Wait for some time if this session is throttled (slowed down).
     */
    public void throttle() {
        if (currentCommandStart == null) {
            currentCommandStart = CurrentTimestamp.get();
        }
        if (throttleNs == 0) {
            return;
        }
        long time = System.nanoTime();
        if (lastThrottle + TimeUnit.MILLISECONDS.toNanos(Constants.THROTTLE_DELAY) > time) {
            return;
        }
        State prevState = this.state.get();
        if (prevState != State.CLOSED) {
            lastThrottle = time + throttleNs;
            try {
                state.compareAndSet(prevState, State.SLEEP);
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(throttleNs));
            } catch (Exception e) {
                // ignore InterruptedException
            } finally {
                state.compareAndSet(State.SLEEP, prevState);
            }
        }
    }

    /**
     * Set the current command of this session. This is done just before
     * executing the statement.
     *
     * @param command the command
     * @param generatedKeysRequest
     *            {@code false} if generated keys are not needed, {@code true} if
     *            generated keys should be configured automatically, {@code int[]}
     *            to specify column indices to return generated keys from, or
     *            {@code String[]} to specify column names to return generated keys
     *            from
     */
    public void setCurrentCommand(Command command, Object generatedKeysRequest) {
        currentCommand = command;
        // Preserve generated keys in case of a new query due to possible nested
        // queries in update
        if (command != null && !command.isQuery()) {
            getGeneratedKeys().clear(generatedKeysRequest);
        }
        if (command != null) {
            if (queryTimeout > 0) {
                currentCommandStart = CurrentTimestamp.get();
                long now = System.nanoTime();
                cancelAtNs = now + TimeUnit.MILLISECONDS.toNanos(queryTimeout);
            } else {
                currentCommandStart = null;
            }
        }
        State currentState = state.get();
        if(currentState != State.CLOSED) {
            state.compareAndSet(currentState, command == null ? State.SLEEP : State.RUNNING);
        }
    }

    /**
     * Check if the current transaction is canceled by calling
     * Statement.cancel() or because a session timeout was set and expired.
     *
     * @throws DbException if the transaction is canceled
     */
    public void checkCanceled() {
        throttle();
        if (cancelAtNs == 0) {
            return;
        }
        long time = System.nanoTime();
        if (time >= cancelAtNs) {
            cancelAtNs = 0;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    /**
     * Get the cancel time.
     *
     * @return the time or 0 if not set
     */
    public long getCancel() {
        return cancelAtNs;
    }

    public Command getCurrentCommand() {
        return currentCommand;
    }

    public ValueTimestampTimeZone getCurrentCommandStart() {
        if (currentCommandStart == null) {
            currentCommandStart = CurrentTimestamp.get();
        }
        return currentCommandStart;
    }

    public boolean getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(boolean b) {
        this.allowLiterals = b;
    }

    public void setCurrentSchema(Schema schema) {
        modificationId++;
        this.currentSchemaName = schema.getName();
    }

    @Override
    public String getCurrentSchemaName() {
        return currentSchemaName;
    }

    @Override
    public void setCurrentSchemaName(String schemaName) {
        Schema schema = database.getSchema(schemaName);
        setCurrentSchema(schema);
    }

    /**
     * Create an internal connection. This connection is used when initializing
     * triggers, and when calling user defined functions.
     *
     * @param columnList if the url should be 'jdbc:columnlist:connection'
     * @return the internal connection
     */
    public JdbcConnection createConnection(boolean columnList) {
        String url;
        if (columnList) {
            url = Constants.CONN_URL_COLUMNLIST;
        } else {
            url = Constants.CONN_URL_INTERNAL;
        }
        return new JdbcConnection(this, getUser().getName(), url);
    }

    @Override
    public DataHandler getDataHandler() {
        return database;
    }

    /**
     * Remember that the given LOB value must be removed at commit.
     *
     * @param v the value
     */
    public void removeAtCommit(Value v) {
        final String key = v.toString();
        if (!v.isLinkedToTable()) {
            DbException.throwInternalError(key);
        }
        if (removeLobMap == null) {
            removeLobMap = new HashMap<>();
        }
        removeLobMap.put(key, v);
    }

    /**
     * Do not remove this LOB value at commit any longer.
     *
     * @param v the value
     */
    public void removeAtCommitStop(Value v) {
        if (removeLobMap != null) {
            removeLobMap.remove(v.toString());
        }
    }

    /**
     * Get the next system generated identifiers. The identifier returned does
     * not occur within the given SQL statement.
     *
     * @param sql the SQL statement
     * @return the new identifier
     */
    public String getNextSystemIdentifier(String sql) {
        String identifier;
        do {
            identifier = SYSTEM_IDENTIFIER_PREFIX + systemIdentifier++;
        } while (sql.contains(identifier));
        return identifier;
    }

    /**
     * Add a procedure to this session.
     *
     * @param procedure the procedure to add
     */
    public void addProcedure(Procedure procedure) {
        if (procedures == null) {
            procedures = database.newStringMap();
        }
        procedures.put(procedure.getName(), procedure);
    }

    /**
     * Remove a procedure from this session.
     *
     * @param name the name of the procedure to remove
     */
    public void removeProcedure(String name) {
        if (procedures != null) {
            procedures.remove(name);
        }
    }

    /**
     * Get the procedure with the given name, or null
     * if none exists.
     *
     * @param name the procedure name
     * @return the procedure or null
     */
    public Procedure getProcedure(String name) {
        if (procedures == null) {
            return null;
        }
        return procedures.get(name);
    }

    public void setSchemaSearchPath(String[] schemas) {
        modificationId++;
        this.schemaSearchPath = schemas;
    }

    public String[] getSchemaSearchPath() {
        return schemaSearchPath;
    }

    @Override
    public int hashCode() {
        return serialId;
    }

    @Override
    public String toString() {
        return "#" + serialId + " (user: " + (user == null ? "<null>" : user.getName()) + ")";
    }

    public void setUndoLogEnabled(boolean b) {
        this.undoLogEnabled = b;
    }

    public void setRedoLogBinary(boolean b) {
        this.redoLogBinary = b;
    }

    public boolean isUndoLogEnabled() {
        return undoLogEnabled;
    }

    /**
     * Begin a transaction.
     */
    public void begin() {
        autoCommitAtTransactionEnd = true;
        autoCommit = false;
    }

    public long getSessionStart() {
        return sessionStart;
    }

    public ValueTimestampTimeZone getTransactionStart() {
        if (transactionStart == null) {
            transactionStart = CurrentTimestamp.get();
        }
        return transactionStart;
    }

    public Table[] getLocks() {
        // copy the data without synchronizing
        ArrayList<Table> copy = new ArrayList<>(locks.size());
        for (Table lock : locks) {
            try {
                copy.add(lock);
            } catch (Exception e) {
                // ignore
                break;
            }
        }
        return copy.toArray(new Table[0]);
    }

    /**
     * Wait if the exclusive mode has been enabled for another session. This
     * method returns as soon as the exclusive mode has been disabled.
     */
    public void waitIfExclusiveModeEnabled() {
        // Even in exclusive mode, we have to let the LOB session proceed, or we
        // will get deadlocks.
        if (database.getLobSession() == this) {
            return;
        }
        while (true) {
            Session exclusive = database.getExclusiveSession();
            if (exclusive == null || exclusive == this) {
                break;
            }
            if (Thread.holdsLock(exclusive)) {
                // if another connection is used within the connection
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Get the view cache for this session. There are two caches: the subquery
     * cache (which is only use for a single query, has no bounds, and is
     * cleared after use), and the cache for regular views.
     *
     * @param subQuery true to get the subquery cache
     * @return the view cache
     */
    public Map<Object, ViewIndex> getViewIndexCache(boolean subQuery) {
        if (subQuery) {
            // for sub-queries we don't need to use LRU because the cache should
            // not grow too large for a single query (we drop the whole cache in
            // the end of prepareLocal)
            if (subQueryIndexCache == null) {
                subQueryIndexCache = new HashMap<>();
            }
            return subQueryIndexCache;
        }
        SmallLRUCache<Object, ViewIndex> cache = viewIndexCache;
        if (cache == null) {
            viewIndexCache = cache = SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
        }
        return cache;
    }

    /**
     * Remember the result set and close it as soon as the transaction is
     * committed (if it needs to be closed). This is done to delete temporary
     * files as soon as possible, and free object ids of temporary tables.
     *
     * @param result the temporary result set
     */
    public void addTemporaryResult(ResultInterface result) {
        if (!result.needToClose()) {
            return;
        }
        if (temporaryResults == null) {
            temporaryResults = new HashSet<>();
        }
        if (temporaryResults.size() < 100) {
            // reference at most 100 result sets to avoid memory problems
            temporaryResults.add(result);
        }
    }

    private void closeTemporaryResults() {
        if (temporaryResults != null) {
            for (ResultInterface result : temporaryResults) {
                result.close();
            }
            temporaryResults = null;
        }
    }

    public void setQueryTimeout(int queryTimeout) {
        int max = database.getSettings().maxQueryTimeout;
        if (max != 0 && (max < queryTimeout || queryTimeout == 0)) {
            // the value must be at most max
            queryTimeout = max;
        }
        this.queryTimeout = queryTimeout;
        // must reset the cancel at here,
        // otherwise it is still used
        this.cancelAtNs = 0;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set the table this session is waiting for, and the thread that is
     * waiting.
     *
     * @param waitForLock the table
     * @param waitForLockThread the current thread (the one that is waiting)
     */
    public void setWaitForLock(Table waitForLock, Thread waitForLockThread) {
        this.waitForLock = waitForLock;
        this.waitForLockThread = waitForLockThread;
    }

    public Table getWaitForLock() {
        return waitForLock;
    }

    public Thread getWaitForLockThread() {
        return waitForLockThread;
    }

    public int getModificationId() {
        return modificationId;
    }

    @Override
    public boolean isReconnectNeeded(boolean write) {
        while (true) {
            boolean reconnect = database.isReconnectNeeded();
            if (reconnect) {
                return true;
            }
            if (write) {
                if (database.beforeWriting()) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public void afterWriting() {
        database.afterWriting();
    }

    @Override
    public SessionInterface reconnect(boolean write) {
        readSessionState();
        close();
        Session newSession = Engine.getInstance().createSession(connectionInfo);
        newSession.sessionState = sessionState;
        newSession.recreateSessionState();
        if (write) {
            while (!newSession.database.beforeWriting()) {
                // wait until we are allowed to write
            }
        }
        return newSession;
    }

    public void setConnectionInfo(ConnectionInfo ci) {
        connectionInfo = ci;
    }

    public Value getTransactionId() {
        if (database.getStore() != null) {
            if (transaction == null || !transaction.hasChanges()) {
                return ValueNull.INSTANCE;
            }
            return ValueString.get(Long.toString(getTransaction().getSequenceNum()));
        }
        if (!database.isPersistent()) {
            return ValueNull.INSTANCE;
        }
        if (undoLog == null || undoLog.size() == 0) {
            return ValueNull.INSTANCE;
        }
        return ValueString.get(firstUncommittedLog + "-" + firstUncommittedPos +
                "-" + id);
    }

    /**
     * Get the next object id.
     *
     * @return the next object id
     */
    public int nextObjectId() {
        return objectId++;
    }

    public boolean isRedoLogBinaryEnabled() {
        return redoLogBinary;
    }

    /**
     * Get the transaction to use for this session.
     *
     * @return the transaction
     */
    public Transaction getTransaction() {
        if (transaction == null) {
            MVTableEngine.Store store = database.getStore();
            if (store != null) {
                if (store.getMvStore().isClosed()) {
                    Throwable backgroundException = database.getBackgroundException();
                    database.shutdownImmediately();
                    throw DbException.get(ErrorCode.DATABASE_IS_CLOSED, backgroundException);
                }
                transaction = store.getTransactionStore().begin(this, this.lockTimeout, id);
            }
            startStatement = -1;
        }
        return transaction;
    }

    private long getStatementSavepoint() {
        if (startStatement == -1) {
            startStatement = getTransaction().setSavepoint();
        }
        return startStatement;
    }

    /**
     * Start a new statement within a transaction.
     */
    public void startStatementWithinTransaction() {
        Transaction transaction = getTransaction();
        if(transaction != null) {
            transaction.markStatementStart();
        }
        startStatement = -1;
    }

    /**
     * Mark the statement as completed. This also close all temporary result
     * set, and deletes all temporary files held by the result sets.
     */
    public void endStatement() {
        if(transaction != null) {
            transaction.markStatementEnd();
        }
        startStatement = -1;
        closeTemporaryResults();
    }

    /**
     * Clear the view cache for this session.
     */
    public void clearViewIndexCache() {
        viewIndexCache = null;
    }

    @Override
    public void addTemporaryLob(Value v) {
        if (!DataType.isLargeObject(v.getValueType())) {
            return;
        }
        if (v.getTableId() == LobStorageFrontend.TABLE_RESULT
                || v.getTableId() == LobStorageFrontend.TABLE_TEMP) {
            if (temporaryResultLobs == null) {
                temporaryResultLobs = new LinkedList<>();
            }
            temporaryResultLobs.add(new TimeoutValue(v));
        } else {
            if (temporaryLobs == null) {
                temporaryLobs = new ArrayList<>();
            }
            temporaryLobs.add(v);
        }
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    /**
     * Mark that the given table needs to be analyzed on commit.
     *
     * @param table the table
     */
    public void markTableForAnalyze(Table table) {
        if (tablesToAnalyze == null) {
            tablesToAnalyze = new HashSet<>();
        }
        tablesToAnalyze.add(table);
    }

    public State getState() {
        return getBlockingSessionId() != 0 ? State.BLOCKED : state.get();
    }

    public int getBlockingSessionId() {
        return transaction == null ? 0 : transaction.getBlockerId();
    }

    @Override
    public void onRollback(MVMap<Object, VersionedValue> map, Object key,
                            VersionedValue existingValue,
                            VersionedValue restoredValue) {
        // Here we are relying on the fact that map which backs table's primary index
        // has the same name as the table itself
        MVTableEngine.Store store = database.getStore();
        if(store != null) {
            MVTable table = store.getTable(map.getName());
            if (table != null) {
                long recKey = ((ValueLong)key).getLong();
                Row oldRow = getRowFromVersionedValue(table, recKey, existingValue);
                Row newRow = getRowFromVersionedValue(table, recKey, restoredValue);
                table.fireAfterRow(this, oldRow, newRow, true);

                if (table.getContainsLargeObject()) {
                    if (oldRow != null) {
                        for (int i = 0, len = oldRow.getColumnCount(); i < len; i++) {
                            Value v = oldRow.getValue(i);
                            if (v.isLinkedToTable()) {
                                removeAtCommit(v);
                            }
                        }
                    }
                    if (newRow != null) {
                        for (int i = 0, len = newRow.getColumnCount(); i < len; i++) {
                            Value v = newRow.getValue(i);
                            if (v.isLinkedToTable()) {
                                removeAtCommitStop(v);
                            }
                        }
                    }
                }
            }
        }
    }

    private static Row getRowFromVersionedValue(MVTable table, long recKey,
                                                VersionedValue versionedValue) {
        Object value = versionedValue == null ? null : versionedValue.getCurrentValue();
        if (value == null) {
            return null;
        }
        Row result;
        if(value instanceof Row) {
            result = (Row) value;
            assert result.getKey() == recKey : result.getKey() + " != " + recKey;
        } else {
            ValueArray array = (ValueArray) value;
            result = table.createRow(array.getList(), 0);
            result.setKey(recKey);
        }
        return result;
    }


    /**
     * Represents a savepoint (a position in a transaction to where one can roll
     * back to).
     */
    public static class Savepoint {

        /**
         * The undo log index.
         */
        int logIndex;

        /**
         * The transaction savepoint id.
         */
        long transactionSavepoint;
    }

    /**
     * An object with a timeout.
     */
    public static class TimeoutValue {

        /**
         * The time when this object was created.
         */
        final long created = System.nanoTime();

        /**
         * The value.
         */
        final Value value;

        TimeoutValue(Value v) {
            this.value = v;
        }

    }

    public ColumnNamerConfiguration getColumnNamerConfiguration() {
        return columnNamerConfiguration;
    }

    public void setColumnNamerConfiguration(ColumnNamerConfiguration columnNamerConfiguration) {
        this.columnNamerConfiguration = columnNamerConfiguration;
    }

    @Override
    public boolean isSupportsGeneratedKeys() {
        return true;
    }

}
