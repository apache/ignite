/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.constraint.Constraint;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.result.SearchRow;
import org.h2.result.SimpleRow;
import org.h2.result.SimpleRowValue;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObjectBase;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.util.New;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This is the base class for most tables.
 * A table contains a list of columns and a list of rows.
 */
public abstract class Table extends SchemaObjectBase {

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_CACHED = 0;

    /**
     * The table type that means this table is a regular persistent table.
     */
    public static final int TYPE_MEMORY = 1;

    /**
     * The columns of this table.
     */
    protected Column[] columns;

    /**
     * The compare mode used for this table.
     */
    protected CompareMode compareMode;

    /**
     * Protected tables are not listed in the meta data and are excluded when
     * using the SCRIPT command.
     */
    protected boolean isHidden;

    private final HashMap<String, Column> columnMap;
    private final boolean persistIndexes;
    private final boolean persistData;
    private ArrayList<TriggerObject> triggers;
    private ArrayList<Constraint> constraints;
    private ArrayList<Sequence> sequences;
    /**
     * views that depend on this table
     */
    private final CopyOnWriteArrayList<TableView> dependentViews = new CopyOnWriteArrayList<>();
    private ArrayList<TableSynonym> synonyms;
    private boolean checkForeignKeyConstraints = true;
    private boolean onCommitDrop, onCommitTruncate;
    private volatile Row nullRow;
    private boolean tableExpression;

    public Table(Schema schema, int id, String name, boolean persistIndexes,
            boolean persistData) {
        columnMap = schema.getDatabase().newStringMap();
        initSchemaObjectBase(schema, id, name, Trace.TABLE);
        this.persistIndexes = persistIndexes;
        this.persistData = persistData;
        compareMode = schema.getDatabase().getCompareMode();
    }

    @Override
    public void rename(String newName) {
        super.rename(newName);
        if (constraints != null) {
            for (Constraint constraint : constraints) {
                constraint.rebuild();
            }
        }
    }

    public boolean isView() {
        return false;
    }

    /**
     * Lock the table for the given session.
     * This method waits until the lock is granted.
     *
     * @param session the session
     * @param exclusive true for write locks, false for read locks
     * @param forceLockEvenInMvcc lock even in the MVCC mode
     * @return true if the table was already exclusively locked by this session.
     * @throws DbException if a lock timeout occurred
     */
    public abstract boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc);

    /**
     * Close the table object and flush changes.
     *
     * @param session the session
     */
    public abstract void close(Session session);

    /**
     * Release the lock for this session.
     *
     * @param s the session
     */
    public abstract void unlock(Session s);

    /**
     * Create an index for this table
     *
     * @param session the session
     * @param indexName the name of the index
     * @param indexId the id
     * @param cols the index columns
     * @param indexType the index type
     * @param create whether this is a new index
     * @param indexComment the comment
     * @return the index
     */
    public abstract Index addIndex(Session session, String indexName,
            int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment);

    /**
     * Get the given row.
     *
     * @param session the session
     * @param key the primary key
     * @return the row
     */
    @SuppressWarnings("unused")
    public Row getRow(Session session, long key) {
        return null;
    }

    /**
     * Remove a row from the table and all indexes.
     *
     * @param session the session
     * @param row the row
     */
    public abstract void removeRow(Session session, Row row);

    /**
     * Remove all rows from the table and indexes.
     *
     * @param session the session
     */
    public abstract void truncate(Session session);

    /**
     * Add a row to the table and all indexes.
     *
     * @param session the session
     * @param row the row
     * @throws DbException if a constraint was violated
     */
    public abstract void addRow(Session session, Row row);

    /**
     * Commit an operation (when using multi-version concurrency).
     *
     * @param operation the operation
     * @param row the row
     */
    @SuppressWarnings("unused")
    public void commit(short operation, Row row) {
        // nothing to do
    }

    /**
     * Check if this table supports ALTER TABLE.
     *
     * @throws DbException if it is not supported
     */
    public abstract void checkSupportAlter();

    /**
     * Get the table type name
     *
     * @return the table type name
     */
    public abstract TableType getTableType();

    /**
     * Get the scan index to iterate through all rows.
     *
     * @param session the session
     * @return the index
     */
    public abstract Index getScanIndex(Session session);

    /**
     * Get the scan index for this table.
     *
     * @param session the session
     * @param masks the search mask
     * @param filters the table filters
     * @param filter the filter index
     * @param sortOrder the sort order
     * @param allColumnsSet all columns
     * @return the scan index
     */
    @SuppressWarnings("unused")
    public Index getScanIndex(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return getScanIndex(session);
    }

    /**
     * Get any unique index for this table if one exists.
     *
     * @return a unique index
     */
    public abstract Index getUniqueIndex();

    /**
     * Get all indexes for this table.
     *
     * @return the list of indexes
     */
    public abstract ArrayList<Index> getIndexes();

    /**
     * Get an index by name.
     *
     * @param indexName the index name to search for
     * @return the found index
     */
    public Index getIndex(String indexName) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getName().equals(indexName)) {
                    return index;
                }
            }
        }
        throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);
    }

    /**
     * Check if this table is locked exclusively.
     *
     * @return true if it is.
     */
    public abstract boolean isLockedExclusively();

    /**
     * Get the last data modification id.
     *
     * @return the modification id
     */
    public abstract long getMaxDataModificationId();

    /**
     * Check if the table is deterministic.
     *
     * @return true if it is
     */
    public abstract boolean isDeterministic();

    /**
     * Check if the row count can be retrieved quickly.
     *
     * @return true if it can
     */
    public abstract boolean canGetRowCount();

    /**
     * Check if this table can be referenced.
     *
     * @return true if it can
     */
    public boolean canReference() {
        return true;
    }

    /**
     * Check if this table can be dropped.
     *
     * @return true if it can
     */
    public abstract boolean canDrop();

    /**
     * Get the row count for this table.
     *
     * @param session the session
     * @return the row count
     */
    public abstract long getRowCount(Session session);

    /**
     * Get the approximated row count for this table.
     *
     * @return the approximated row count
     */
    public abstract long getRowCountApproximation();

    public abstract long getDiskSpaceUsed();

    /**
     * Get the row id column if this table has one.
     *
     * @return the row id column, or null
     */
    public Column getRowIdColumn() {
        return null;
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    /**
     * Check whether the table (or view) contains no columns that prevent index
     * conditions to be used. For example, a view that contains the ROWNUM()
     * pseudo-column prevents this.
     *
     * @return true if the table contains no query-comparable column
     */
    public boolean isQueryComparable() {
        return true;
    }

    /**
     * Add all objects that this table depends on to the hash set.
     *
     * @param dependencies the current set of dependencies
     */
    public void addDependencies(HashSet<DbObject> dependencies) {
        if (dependencies.contains(this)) {
            // avoid endless recursion
            return;
        }
        if (sequences != null) {
            dependencies.addAll(sequences);
        }
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(
                dependencies);
        for (Column col : columns) {
            col.isEverything(visitor);
        }
        if (constraints != null) {
            for (Constraint c : constraints) {
                c.isEverything(visitor);
            }
        }
        dependencies.add(this);
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = New.arrayList();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            children.addAll(indexes);
        }
        if (constraints != null) {
            children.addAll(constraints);
        }
        if (triggers != null) {
            children.addAll(triggers);
        }
        if (sequences != null) {
            children.addAll(sequences);
        }
        children.addAll(dependentViews);
        if (synonyms != null) {
            children.addAll(synonyms);
        }
        ArrayList<Right> rights = database.getAllRights();
        for (Right right : rights) {
            if (right.getGrantedObject() == this) {
                children.add(right);
            }
        }
        return children;
    }

    protected void setColumns(Column[] columns) {
        this.columns = columns;
        if (columnMap.size() > 0) {
            columnMap.clear();
        }
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            int dataType = col.getType();
            if (dataType == Value.UNKNOWN) {
                throw DbException.get(
                        ErrorCode.UNKNOWN_DATA_TYPE_1, col.getSQL());
            }
            col.setTable(this, i);
            String columnName = col.getName();
            if (columnMap.get(columnName) != null) {
                throw DbException.get(
                        ErrorCode.DUPLICATE_COLUMN_NAME_1, columnName);
            }
            columnMap.put(columnName, col);
        }
    }

    /**
     * Rename a column of this table.
     *
     * @param column the column to rename
     * @param newName the new column name
     */
    public void renameColumn(Column column, String newName) {
        for (Column c : columns) {
            if (c == column) {
                continue;
            }
            if (c.getName().equals(newName)) {
                throw DbException.get(
                        ErrorCode.DUPLICATE_COLUMN_NAME_1, newName);
            }
        }
        columnMap.remove(column.getName());
        column.rename(newName);
        columnMap.put(newName, column);
    }

    /**
     * Check if the table is exclusively locked by this session.
     *
     * @param session the session
     * @return true if it is
     */
    @SuppressWarnings("unused")
    public boolean isLockedExclusivelyBy(Session session) {
        return false;
    }

    /**
     * Update a list of rows in this table.
     *
     * @param prepared the prepared statement
     * @param session the session
     * @param rows a list of row pairs of the form old row, new row, old row,
     *            new row,...
     */
    public void updateRows(Prepared prepared, Session session, RowList rows) {
        // in case we need to undo the update
        Session.Savepoint rollback = session.setSavepoint();
        // remove the old rows
        int rowScanCount = 0;
        for (rows.reset(); rows.hasNext();) {
            if ((++rowScanCount & 127) == 0) {
                prepared.checkCanceled();
            }
            Row o = rows.next();
            rows.next();
            try {
                removeRow(session, o);
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1) {
                    session.rollbackTo(rollback, false);
                    session.startStatementWithinTransaction();
                    rollback = session.setSavepoint();
                }
                throw e;
            }
            session.log(this, UndoLogRecord.DELETE, o);
        }
        // add the new rows
        for (rows.reset(); rows.hasNext();) {
            if ((++rowScanCount & 127) == 0) {
                prepared.checkCanceled();
            }
            rows.next();
            Row n = rows.next();
            try {
                addRow(session, n);
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1) {
                    session.rollbackTo(rollback, false);
                    session.startStatementWithinTransaction();
                    rollback = session.setSavepoint();
                }
                throw e;
            }
            session.log(this, UndoLogRecord.INSERT, n);
        }
    }

    public CopyOnWriteArrayList<TableView> getDependentViews() {
        return dependentViews;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        while (!dependentViews.isEmpty()) {
            TableView view = dependentViews.get(0);
            dependentViews.remove(0);
            database.removeSchemaObject(session, view);
        }
        while (synonyms != null && !synonyms.isEmpty()) {
            TableSynonym synonym = synonyms.remove(0);
            database.removeSchemaObject(session, synonym);
        }
        while (triggers != null && !triggers.isEmpty()) {
            TriggerObject trigger = triggers.remove(0);
            database.removeSchemaObject(session, trigger);
        }
        while (constraints != null && !constraints.isEmpty()) {
            Constraint constraint = constraints.remove(0);
            database.removeSchemaObject(session, constraint);
        }
        for (Right right : database.getAllRights()) {
            if (right.getGrantedObject() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        // must delete sequences later (in case there is a power failure
        // before removing the table object)
        while (sequences != null && !sequences.isEmpty()) {
            Sequence sequence = sequences.remove(0);
            // only remove if no other table depends on this sequence
            // this is possible when calling ALTER TABLE ALTER COLUMN
            if (database.getDependentTable(sequence, this) == null) {
                database.removeSchemaObject(session, sequence);
            }
        }
    }

    /**
     * Check that these columns are not referenced by a multi-column constraint
     * or multi-column index. If it is, an exception is thrown. Single-column
     * references and indexes are dropped.
     *
     * @param session the session
     * @param columnsToDrop the columns to drop
     * @throws DbException if the column is referenced by multi-column
     *             constraints or indexes
     */
    public void dropMultipleColumnsConstraintsAndIndexes(Session session,
            ArrayList<Column> columnsToDrop) {
        HashSet<Constraint> constraintsToDrop = new HashSet<>();
        if (constraints != null) {
            for (Column col : columnsToDrop) {
                for (Constraint constraint : constraints) {
                    HashSet<Column> columns = constraint.getReferencedColumns(this);
                    if (!columns.contains(col)) {
                        continue;
                    }
                    if (columns.size() == 1) {
                        constraintsToDrop.add(constraint);
                    } else {
                        throw DbException.get(
                                ErrorCode.COLUMN_IS_REFERENCED_1, constraint.getSQL());
                    }
                }
            }
        }
        HashSet<Index> indexesToDrop = new HashSet<>();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (Column col : columnsToDrop) {
                for (Index index : indexes) {
                    if (index.getCreateSQL() == null) {
                        continue;
                    }
                    if (index.getColumnIndex(col) < 0) {
                        continue;
                    }
                    if (index.getColumns().length == 1) {
                        indexesToDrop.add(index);
                    } else {
                        throw DbException.get(
                                ErrorCode.COLUMN_IS_REFERENCED_1, index.getSQL());
                    }
                }
            }
        }
        for (Constraint c : constraintsToDrop) {
            session.getDatabase().removeSchemaObject(session, c);
        }
        for (Index i : indexesToDrop) {
            // the index may already have been dropped when dropping the
            // constraint
            if (getIndexes().contains(i)) {
                session.getDatabase().removeSchemaObject(session, i);
            }
        }
    }

    public Row getTemplateRow() {
        return database.createRow(new Value[columns.length], Row.MEMORY_CALCULATE);
    }

    /**
     * Get a new simple row object.
     *
     * @param singleColumn if only one value need to be stored
     * @return the simple row object
     */
    public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        if (singleColumn) {
            return new SimpleRowValue(columns.length);
        }
        return new SimpleRow(new Value[columns.length]);
    }

    Row getNullRow() {
        Row row = nullRow;
        if (row == null) {
            // Here can be concurrently produced more than one row, but it must
            // be ok.
            Value[] values = new Value[columns.length];
            Arrays.fill(values, ValueNull.INSTANCE);
            nullRow = row = database.createRow(values, 1);
        }
        return row;
    }

    public Column[] getColumns() {
        return columns;
    }

    @Override
    public int getType() {
        return DbObject.TABLE_OR_VIEW;
    }

    /**
     * Get the column at the given index.
     *
     * @param index the column index (0, 1,...)
     * @return the column
     */
    public Column getColumn(int index) {
        return columns[index];
    }

    /**
     * Get the column with the given name.
     *
     * @param columnName the column name
     * @return the column
     * @throws DbException if the column was not found
     */
    public Column getColumn(String columnName) {
        Column column = columnMap.get(columnName);
        if (column == null) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    /**
     * Does the column with the given name exist?
     *
     * @param columnName the column name
     * @return true if the column exists
     */
    public boolean doesColumnExist(String columnName) {
        return columnMap.containsKey(columnName);
    }

    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param allColumnsSet the set of all columns
     * @return the plan item
     */
    public PlanItem getBestPlanItem(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        PlanItem item = new PlanItem();
        item.setIndex(getScanIndex(session));
        item.cost = item.getIndex().getCost(session, null, filters, filter, null, allColumnsSet);
        Trace t = session.getTrace();
        if (t.isDebugEnabled()) {
            t.debug("Table      :     potential plan item cost {0} index {1}",
                    item.cost, item.getIndex().getPlanSQL());
        }
        ArrayList<Index> indexes = getIndexes();
        IndexHints indexHints = getIndexHints(filters, filter);

        if (indexes != null && masks != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);

                if (isIndexExcludedByHints(indexHints, index)) {
                    continue;
                }

                double cost = index.getCost(session, masks, filters, filter,
                        sortOrder, allColumnsSet);
                if (t.isDebugEnabled()) {
                    t.debug("Table      :     potential plan item cost {0} index {1}",
                            cost, index.getPlanSQL());
                }
                if (cost < item.cost) {
                    item.cost = cost;
                    item.setIndex(index);
                }
            }
        }
        return item;
    }

    private static boolean isIndexExcludedByHints(IndexHints indexHints, Index index) {
        return indexHints != null && !indexHints.allowIndex(index);
    }

    private static IndexHints getIndexHints(TableFilter[] filters, int filter) {
        return filters == null ? null : filters[filter].getIndexHints();
    }

    /**
     * Get the primary key index if there is one, or null if there is none.
     *
     * @return the primary key index or null
     */
    public Index findPrimaryKey() {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (Index idx : indexes) {
                if (idx.getIndexType().isPrimaryKey()) {
                    return idx;
                }
            }
        }
        return null;
    }

    public Index getPrimaryKey() {
        Index index = findPrimaryKey();
        if (index != null) {
            return index;
        }
        throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1,
                Constants.PREFIX_PRIMARY_KEY);
    }

    /**
     * Validate all values in this row, convert the values if required, and
     * update the sequence values if required. This call will also set the
     * default values if required and set the computed column if there are any.
     *
     * @param session the session
     * @param row the row
     */
    public void validateConvertUpdateSequence(Session session, Row row) {
        for (int i = 0; i < columns.length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            Value v2;
            if (column.getComputed()) {
                // force updating the value
                value = null;
                v2 = column.computeValue(session, row);
            }
            v2 = column.validateConvertUpdateSequence(session, value);
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
    }

    private static void remove(ArrayList<? extends DbObject> list, DbObject obj) {
        if (list != null) {
            list.remove(obj);
        }
    }

    /**
     * Remove the given index from the list.
     *
     * @param index the index to remove
     */
    public void removeIndex(Index index) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            remove(indexes, index);
            if (index.getIndexType().isPrimaryKey()) {
                for (Column col : index.getColumns()) {
                    col.setPrimaryKey(false);
                }
            }
        }
    }

    /**
     * Remove the given view from the dependent views list.
     *
     * @param view the view to remove
     */
    public void removeDependentView(TableView view) {
        dependentViews.remove(view);
    }

    /**
     * Remove the given view from the list.
     *
     * @param synonym the synonym to remove
     */
    public void removeSynonym(TableSynonym synonym) {
        remove(synonyms, synonym);
    }

    /**
     * Remove the given constraint from the list.
     *
     * @param constraint the constraint to remove
     */
    public void removeConstraint(Constraint constraint) {
        remove(constraints, constraint);
    }

    /**
     * Remove a sequence from the table. Sequences are used as identity columns.
     *
     * @param sequence the sequence to remove
     */
    public final void removeSequence(Sequence sequence) {
        remove(sequences, sequence);
    }

    /**
     * Remove the given trigger from the list.
     *
     * @param trigger the trigger to remove
     */
    public void removeTrigger(TriggerObject trigger) {
        remove(triggers, trigger);
    }

    /**
     * Add a view to this table.
     *
     * @param view the view to add
     */
    public void addDependentView(TableView view) {
        dependentViews.add(view);
    }

    /**
     * Add a synonym to this table.
     *
     * @param synonym the synonym to add
     */
    public void addSynonym(TableSynonym synonym) {
        synonyms = add(synonyms, synonym);
    }

    /**
     * Add a constraint to the table.
     *
     * @param constraint the constraint to add
     */
    public void addConstraint(Constraint constraint) {
        if (constraints == null || !constraints.contains(constraint)) {
            constraints = add(constraints, constraint);
        }
    }

    public ArrayList<Constraint> getConstraints() {
        return constraints;
    }

    /**
     * Add a sequence to this table.
     *
     * @param sequence the sequence to add
     */
    public void addSequence(Sequence sequence) {
        sequences = add(sequences, sequence);
    }

    /**
     * Add a trigger to this table.
     *
     * @param trigger the trigger to add
     */
    public void addTrigger(TriggerObject trigger) {
        triggers = add(triggers, trigger);
    }

    private static <T> ArrayList<T> add(ArrayList<T> list, T obj) {
        if (list == null) {
            list = New.arrayList();
        }
        // self constraints are two entries in the list
        list.add(obj);
        return list;
    }

    /**
     * Fire the triggers for this table.
     *
     * @param session the session
     * @param type the trigger type
     * @param beforeAction whether 'before' triggers should be called
     */
    public void fire(Session session, int type, boolean beforeAction) {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                trigger.fire(session, type, beforeAction);
            }
        }
    }

    /**
     * Check whether this table has a select trigger.
     *
     * @return true if it has
     */
    public boolean hasSelectTrigger() {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                if (trigger.isSelectTrigger()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if row based triggers or constraints are defined.
     * In this case the fire after and before row methods need to be called.
     *
     *  @return if there are any triggers or rows defined
     */
    public boolean fireRow() {
        return (constraints != null && !constraints.isEmpty()) ||
                (triggers != null && !triggers.isEmpty());
    }

    /**
     * Fire all triggers that need to be called before a row is updated.
     *
     * @param session the session
     * @param oldRow the old data or null for an insert
     * @param newRow the new data or null for a delete
     * @return true if no further action is required (for 'instead of' triggers)
     */
    public boolean fireBeforeRow(Session session, Row oldRow, Row newRow) {
        boolean done = fireRow(session, oldRow, newRow, true, false);
        fireConstraints(session, oldRow, newRow, true);
        return done;
    }

    private void fireConstraints(Session session, Row oldRow, Row newRow,
            boolean before) {
        if (constraints != null) {
            // don't use enhanced for loop to avoid creating objects
            for (Constraint constraint : constraints) {
                if (constraint.isBefore() == before) {
                    constraint.checkRow(session, this, oldRow, newRow);
                }
            }
        }
    }

    /**
     * Fire all triggers that need to be called after a row is updated.
     *
     *  @param session the session
     *  @param oldRow the old data or null for an insert
     *  @param newRow the new data or null for a delete
     *  @param rollback when the operation occurred within a rollback
     */
    public void fireAfterRow(Session session, Row oldRow, Row newRow,
            boolean rollback) {
        fireRow(session, oldRow, newRow, false, rollback);
        if (!rollback) {
            fireConstraints(session, oldRow, newRow, false);
        }
    }

    private boolean fireRow(Session session, Row oldRow, Row newRow,
            boolean beforeAction, boolean rollback) {
        if (triggers != null) {
            for (TriggerObject trigger : triggers) {
                boolean done = trigger.fireRow(session, this, oldRow, newRow, beforeAction, rollback);
                if (done) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isGlobalTemporary() {
        return false;
    }

    /**
     * Check if this table can be truncated.
     *
     * @return true if it can
     */
    public boolean canTruncate() {
        return false;
    }

    /**
     * Enable or disable foreign key constraint checking for this table.
     *
     * @param session the session
     * @param enabled true if checking should be enabled
     * @param checkExisting true if existing rows must be checked during this
     *            call
     */
    public void setCheckForeignKeyConstraints(Session session, boolean enabled,
            boolean checkExisting) {
        if (enabled && checkExisting) {
            if (constraints != null) {
                for (Constraint c : constraints) {
                    c.checkExistingData(session);
                }
            }
        }
        checkForeignKeyConstraints = enabled;
    }

    public boolean getCheckForeignKeyConstraints() {
        return checkForeignKeyConstraints;
    }

    /**
     * Get the index that has the given column as the first element.
     * This method returns null if no matching index is found.
     *
     * @param column the column
     * @param needGetFirstOrLast if the returned index must be able
     *          to do {@link Index#canGetFirstOrLast()}
     * @param needFindNext if the returned index must be able to do
     *          {@link Index#findNext(Session, SearchRow, SearchRow)}
     * @return the index or null
     */
    public Index getIndexForColumn(Column column,
            boolean needGetFirstOrLast, boolean needFindNext) {
        ArrayList<Index> indexes = getIndexes();
        Index result = null;
        if (indexes != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (needGetFirstOrLast && !index.canGetFirstOrLast()) {
                    continue;
                }
                if (needFindNext && !index.canFindNext()) {
                    continue;
                }
                // choose the minimal covering index with the needed first
                // column to work consistently with execution plan from
                // Optimizer
                if (index.isFirstColumn(column) && (result == null ||
                        result.getColumns().length > index.getColumns().length)) {
                    result = index;
                }
            }
        }
        return result;
    }

    public boolean getOnCommitDrop() {
        return onCommitDrop;
    }

    public void setOnCommitDrop(boolean onCommitDrop) {
        this.onCommitDrop = onCommitDrop;
    }

    public boolean getOnCommitTruncate() {
        return onCommitTruncate;
    }

    public void setOnCommitTruncate(boolean onCommitTruncate) {
        this.onCommitTruncate = onCommitTruncate;
    }

    /**
     * If the index is still required by a constraint, transfer the ownership to
     * it. Otherwise, the index is removed.
     *
     * @param session the session
     * @param index the index that is no longer required
     */
    public void removeIndexOrTransferOwnership(Session session, Index index) {
        boolean stillNeeded = false;
        if (constraints != null) {
            for (Constraint cons : constraints) {
                if (cons.usesIndex(index)) {
                    cons.setIndexOwner(index);
                    database.updateMeta(session, cons);
                    stillNeeded = true;
                }
            }
        }
        if (!stillNeeded) {
            database.removeSchemaObject(session, index);
        }
    }

    /**
     * Check if a deadlock occurred. This method is called recursively. There is
     * a circle if the session to be tested has already being visited. If this
     * session is part of the circle (if it is the clash session), the method
     * must return an empty object array. Once a deadlock has been detected, the
     * methods must add the session to the list. If this session is not part of
     * the circle, or if no deadlock is detected, this method returns null.
     *
     * @param session the session to be tested for
     * @param clash set with sessions already visited, and null when starting
     *            verification
     * @param visited set with sessions already visited, and null when starting
     *            verification
     * @return an object array with the sessions involved in the deadlock, or
     *         null
     */
    @SuppressWarnings("unused")
    public ArrayList<Session> checkDeadlock(Session session, Session clash,
            Set<Session> visited) {
        return null;
    }

    public boolean isPersistIndexes() {
        return persistIndexes;
    }

    public boolean isPersistData() {
        return persistData;
    }

    /**
     * Compare two values with the current comparison mode. The values may be of
     * different type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSafe(Value a, Value b) {
        if (a == b) {
            return 0;
        }
        int dataType = Value.getHigherOrder(a.getType(), b.getType());
        a = a.convertTo(dataType);
        b = b.convertTo(dataType);
        return a.compareTypeSafe(b, compareMode);
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    /**
     * Tests if the table can be written. Usually, this depends on the
     * database.checkWritingAllowed method, but some tables (eg. TableLink)
     * overwrite this default behaviour.
     */
    public void checkWritingAllowed() {
        database.checkWritingAllowed();
    }

    private static Value getGeneratedValue(Session session, Column column, Expression expression) {
        Value v;
        if (expression == null) {
            v = column.validateConvertUpdateSequence(session, null);
        } else {
            v = expression.getValue(session);
        }
        return column.convert(v);
    }

    /**
     * Get or generate a default value for the given column.
     *
     * @param session the session
     * @param column the column
     * @return the value
     */
    public Value getDefaultValue(Session session, Column column) {
        return getGeneratedValue(session, column, column.getDefaultExpression());
    }

    /**
     * Generates on update value for the given column.
     *
     * @param session the session
     * @param column the column
     * @return the value
     */
    public Value getOnUpdateValue(Session session, Column column) {
        return getGeneratedValue(session, column, column.getOnUpdateExpression());
    }

    @Override
    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        this.isHidden = hidden;
    }

    public boolean isMVStore() {
        return false;
    }

    public void setTableExpression(boolean tableExpression) {
        this.tableExpression = tableExpression;
    }

    public boolean isTableExpression() {
        return tableExpression;
    }
}
