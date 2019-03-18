/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.h2.api.ErrorCode;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.ddl.Analyze;
import org.h2.command.dml.Query;
import org.h2.command.dml.SetTypes;
import org.h2.constraint.Constraint;
import org.h2.index.Index;
import org.h2.index.ViewIndex;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.mvstore.db.MVTable;
import org.h2.mvstore.db.TransactionStore.Change;
import org.h2.mvstore.db.TransactionStore.Transaction;
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
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public class Session extends SessionWithState {

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
    private final ArrayList<Table> locks = New.arrayList();
    private final UndoLog undoLog;
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
    private boolean closed;
    private final long sessionStart = System.currentTimeMillis();
    private long transactionStart;
    private long currentCommandStart;
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
    private int parsingView;
    private final Deque<String> viewNameStack = new ArrayDeque<>();
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
    private long startStatement = -1;

    public Session(Database database, User user, int id) {
        this.database = database;
        this.queryTimeout = database.getSettings().maxQueryTimeout;
        this.queryCacheSize = database.getSettings().queryCacheSize;
        this.undoLog = new UndoLog(this);
        this.user = user;
        this.id = id;
        Setting setting = database.findSetting(
                SetTypes.getTypeName(SetTypes.DEFAULT_LOCK_TIMEOUT));
        this.lockTimeout = setting == null ?
                Constants.INITIAL_LOCK_TIMEOUT : setting.getIntValue();
        this.currentSchemaName = Constants.SCHEMA_MAIN;
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
        // It can be recursive, thus implemented as counter.
        this.parsingView += parsingView ? 1 : -1;
        assert this.parsingView >= 0;
        if (parsingView) {
            viewNameStack.push(viewName);
        } else {
            assert viewName.equals(viewNameStack.peek());
            viewNameStack.pop();
        }
    }
    public String getParsingCreateViewName() {
        if (viewNameStack.isEmpty()) {
            return null;
        }
        return viewNameStack.peek();
    }

    public boolean isParsingCreateView() {
        assert parsingView >= 0;
        return parsingView != 0;
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
            return New.arrayList();
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
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1,
                    table.getSQL()+" AS "+table.getName());
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
        database.lockMeta(this);
        modificationId++;
        localTempTables.remove(table.getName());
        synchronized (database) {
            table.removeChildrenAndResources(this);
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
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1,
                    index.getSQL());
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
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1,
                    constraint.getSQL());
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
        if (closed) {
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
        transactionStart = 0;
        if (transaction != null) {
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
            transaction = null;
        }
        if (containsUncommitted()) {
            // need to commit even if rollback is not possible
            // (create/drop table and so on)
            database.commit(this);
        }
        removeTemporaryLobs(true);
        if (undoLog.size() > 0) {
            // commit the rows when using MVCC
            if (database.isMultiVersion()) {
                ArrayList<Row> rows = New.arrayList();
                synchronized (database) {
                    while (undoLog.size() > 0) {
                        UndoLogRecord entry = undoLog.getLast();
                        entry.commit();
                        rows.add(entry.getRow());
                        undoLog.removeLast(false);
                    }
                    for (Row r : rows) {
                        r.commit();
                    }
                }
            }
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

        int rows = getDatabase().getSettings().analyzeSample / 10;
        if (tablesToAnalyze != null) {
            for (Table table : tablesToAnalyze) {
                Analyze.analyzeTable(this, table, rows, false);
            }
            // analyze can lock the meta
            database.unlockMeta(this);
        }
        tablesToAnalyze = null;

        endTransaction();
    }

    private void removeTemporaryLobs(boolean onTimeout) {
        if (SysProperties.CHECK2) {
            if (this == getDatabase().getLobSession()
                    && !Thread.holdsLock(this) && !Thread.holdsLock(getDatabase())) {
                throw DbException.throwInternalError();
            }
        }
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
            if (database.getMvStore() == null) {
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
    }

    /**
     * Fully roll back the current transaction.
     */
    public void rollback() {
        checkCommitRollback();
        currentTransactionName = null;
        transactionStart = 0;
        boolean needCommit = false;
        if (undoLog.size() > 0) {
            rollbackTo(null, false);
            needCommit = true;
        }
        if (transaction != null) {
            rollbackTo(null, false);
            needCommit = true;
            // rollback stored the undo operations in the transaction
            // committing will end the transaction
            transaction.commit();
            transaction = null;
        }
        if (!locks.isEmpty() || needCommit) {
            database.commit(this);
        }
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
     * @param trimToSize if the list should be trimmed
     */
    public void rollbackTo(Savepoint savepoint, boolean trimToSize) {
        int index = savepoint == null ? 0 : savepoint.logIndex;
        while (undoLog.size() > index) {
            UndoLogRecord entry = undoLog.getLast();
            entry.undo(this);
            undoLog.removeLast(trimToSize);
        }
        if (transaction != null) {
            long savepointId = savepoint == null ? 0 : savepoint.transactionSavepoint;
            HashMap<String, MVTable> tableMap =
                    database.getMvStore().getTables();
            Iterator<Change> it = transaction.getChanges(savepointId);
            while (it.hasNext()) {
                Change c = it.next();
                MVTable t = tableMap.get(c.mapName);
                if (t != null) {
                    long key = ((ValueLong) c.key).getLong();
                    ValueArray value = (ValueArray) c.value;
                    short op;
                    Row row;
                    if (value == null) {
                        op = UndoLogRecord.INSERT;
                        row = t.getRow(this, key);
                    } else {
                        op = UndoLogRecord.DELETE;
                        row = createRow(value.getList(), Row.MEMORY_CALCULATE);
                    }
                    row.setKey(key);
                    UndoLogRecord log = new UndoLogRecord(t, op, row);
                    log.undo(this);
                }
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
    }

    @Override
    public boolean hasPendingTransaction() {
        return undoLog.size() > 0;
    }

    /**
     * Create a savepoint to allow rolling back to this state.
     *
     * @return the savepoint
     */
    public Savepoint setSavepoint() {
        Savepoint sp = new Savepoint();
        sp.logIndex = undoLog.size();
        if (database.getMvStore() != null) {
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
        if (!closed) {
            try {
                database.checkPowerOff();

                // release any open table locks
                rollback();

                removeTemporaryLobs(false);
                cleanTempTables(true);
                undoLog.clear();
                // Table#removeChildrenAndResources can take the meta lock,
                // and we need to unlock before we call removeSession(), which might
                // want to take the meta lock using the system session.
                database.unlockMeta(this);
                database.removeSession(this);
            } finally {
                closed = true;
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
                        !database.isMultiVersion()) {
                    TableType tableType = log.getTable().getTableType();
                    if (!locks.contains(log.getTable())
                            && TableType.TABLE_LINK != tableType
                            && TableType.EXTERNAL_TABLE_ENGINE != tableType) {
                        DbException.throwInternalError("" + tableType);
                    }
                }
            }
            undoLog.add(log);
        } else {
            if (database.isMultiVersion()) {
                // see also UndoLogRecord.commit
                ArrayList<Index> indexes = table.getIndexes();
                for (Index index : indexes) {
                    index.commit(operation, row);
                }
                row.commit();
            }
        }
    }

    /**
     * Unlock all read locks. This is done if the transaction isolation mode is
     * READ_COMMITTED.
     */
    public void unlockReadLocks() {
        if (database.isMultiVersion()) {
            // MVCC: keep shared locks (insert / update / delete)
            return;
        }
        // locks is modified in the loop
        for (int i = 0; i < locks.size(); i++) {
            Table t = locks.get(i);
            if (!t.isLockedExclusively()) {
                synchronized (database) {
                    t.unlock(this);
                    locks.remove(i);
                }
                i--;
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
        if (SysProperties.CHECK) {
            if (undoLog.size() > 0) {
                DbException.throwInternalError();
            }
        }
        if (!locks.isEmpty()) {
            // don't use the enhanced for loop to save memory
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
            synchronized (database) {
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
        if (trace != null && !closed) {
            return trace;
        }
        String traceModuleName = "jdbc[" + id + "]";
        if (closed) {
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
        if (database.getMvStore() != null) {
            return transaction != null;
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
        Savepoint sp = new Savepoint();
        sp.logIndex = undoLog.size();
        if (database.getMvStore() != null) {
            sp.transactionSavepoint = getStatementSavepoint();
        }
        savepoints.put(name, sp);
    }

    /**
     * Undo all operations back to the log position of the given savepoint.
     *
     * @param name the savepoint name
     */
    public void rollbackToSavepoint(String name) {
        checkCommitRollback();
        currentTransactionName = null;
        transactionStart = 0;
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        Savepoint savepoint = savepoints.get(name);
        if (savepoint == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        rollbackTo(savepoint, false);
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
        return closed;
    }

    public void setThrottle(int throttle) {
        this.throttleNs = TimeUnit.MILLISECONDS.toNanos(throttle);
    }

    /**
     * Wait for some time if this session is throttled (slowed down).
     */
    public void throttle() {
        if (currentCommandStart == 0) {
            currentCommandStart = System.currentTimeMillis();
        }
        if (throttleNs == 0) {
            return;
        }
        long time = System.nanoTime();
        if (lastThrottle + TimeUnit.MILLISECONDS.toNanos(Constants.THROTTLE_DELAY) > time) {
            return;
        }
        lastThrottle = time + throttleNs;
        try {
            Thread.sleep(TimeUnit.NANOSECONDS.toMillis(throttleNs));
        } catch (Exception e) {
            // ignore InterruptedException
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
        this.currentCommand = command;
        // Preserve generated keys in case of a new query due to possible nested
        // queries in update
        if (command != null && !command.isQuery()) {
            getGeneratedKeys().clear(generatedKeysRequest);
        }
        if (queryTimeout > 0 && command != null) {
            currentCommandStart = System.currentTimeMillis();
            long now = System.nanoTime();
            cancelAtNs = now + TimeUnit.MILLISECONDS.toNanos(queryTimeout);
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

    public long getCurrentCommandStart() {
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
        if (SysProperties.CHECK && !v.isLinkedToTable()) {
            DbException.throwInternalError(v.toString());
        }
        if (removeLobMap == null) {
            removeLobMap = new HashMap<>();
        }
        removeLobMap.put(v.toString(), v);
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
        return "#" + serialId + " (user: " + user.getName() + ")";
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

    public long getTransactionStart() {
        if (transactionStart == 0) {
            transactionStart = System.currentTimeMillis();
        }
        return transactionStart;
    }

    public Table[] getLocks() {
        // copy the data without synchronizing
        ArrayList<Table> copy = New.arrayList();
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
        if (database.getMvStore() != null) {
            if (transaction == null) {
                return ValueNull.INSTANCE;
            }
            return ValueString.get(Long.toString(getTransaction().getId()));
        }
        if (!database.isPersistent()) {
            return ValueNull.INSTANCE;
        }
        if (undoLog.size() == 0) {
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
            if (database.getMvStore().getStore().isClosed()) {
                database.shutdownImmediately();
                throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
            }
            transaction = database.getMvStore().getTransactionStore().begin();
            startStatement = -1;
        }
        return transaction;
    }

    public long getStatementSavepoint() {
        if (startStatement == -1) {
            startStatement = getTransaction().setSavepoint();
        }
        return startStatement;
    }

    /**
     * Start a new statement within a transaction.
     */
    public void startStatementWithinTransaction() {
        startStatement = -1;
    }

    /**
     * Mark the statement as completed. This also close all temporary result
     * set, and deletes all temporary files held by the result sets.
     */
    public void endStatement() {
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
        if (v.getType() != Value.CLOB && v.getType() != Value.BLOB) {
            return;
        }
        if (v.getTableId() == LobStorageFrontend.TABLE_RESULT ||
                v.getTableId() == LobStorageFrontend.TABLE_TEMP) {
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
