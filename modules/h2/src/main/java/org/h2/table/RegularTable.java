/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.command.ddl.CreateTableData;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Most tables are an instance of this class. For this table, the data is stored
 * in the database. The actual data is not kept here, instead it is kept in the
 * indexes. There is at least one index, the scan index.
 */
public abstract class RegularTable extends TableBase {

    /**
     * Appends the specified rows to the specified index.
     *
     * @param session
     *            the session
     * @param list
     *            the rows, list is cleared on completion
     * @param index
     *            the index to append to
     */
    protected static void addRowsToIndex(Session session, ArrayList<Row> list, Index index) {
        sortRows(list, index);
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    /**
     * Formats details of a deadlock.
     *
     * @param sessions
     *            the list of sessions
     * @param exclusive
     *            true if waiting for exclusive lock, false otherwise
     * @return formatted details of a deadlock
     */
    protected static String getDeadlockDetails(ArrayList<Session> sessions, boolean exclusive) {
        // We add the thread details here to make it easier for customers to
        // match up these error messages with their own logs.
        StringBuilder builder = new StringBuilder();
        for (Session s : sessions) {
            Table lock = s.getWaitForLock();
            Thread thread = s.getWaitForLockThread();
            builder.append("\nSession ").append(s.toString()).append(" on thread ").append(thread.getName())
                    .append(" is waiting to lock ").append(lock.toString())
                    .append(exclusive ? " (exclusive)" : " (shared)").append(" while locking ");
            Table[] locks = s.getLocks();
            for (int i = 0, length = locks.length; i < length; i++) {
                Table t = locks[i];
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(t.toString());
                if (t instanceof RegularTable) {
                    if (((RegularTable) t).lockExclusiveSession == s) {
                        builder.append(" (exclusive)");
                    } else {
                        builder.append(" (shared)");
                    }
                }
            }
            builder.append('.');
        }
        return builder.toString();
    }

    /**
     * Sorts the specified list of rows for a specified index.
     *
     * @param list
     *            the list of rows
     * @param index
     *            the index to sort for
     */
    protected static void sortRows(ArrayList<? extends SearchRow> list, final Index index) {
        Collections.sort(list, new Comparator<SearchRow>() {
            @Override
            public int compare(SearchRow r1, SearchRow r2) {
                return index.compareRows(r1, r2);
            }
        });
    }

    /**
     * Whether the table contains a CLOB or BLOB.
     */
    protected final boolean containsLargeObject;

    /**
     * The session (if any) that has exclusively locked this table.
     */
    protected volatile Session lockExclusiveSession;

    /**
     * The set of sessions (if any) that have a shared lock on the table. Here
     * we are using using a ConcurrentHashMap as a set, as there is no
     * ConcurrentHashSet.
     */
    protected final ConcurrentHashMap<Session, Session> lockSharedSessions = new ConcurrentHashMap<>();

    private Column rowIdColumn;

    protected RegularTable(CreateTableData data) {
        super(data);
        this.isHidden = data.isHidden;
        boolean b = false;
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType().getValueType())) {
                b = true;
                break;
            }
        }
        containsLargeObject = b;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public boolean canTruncate() {
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (Constraint c : constraints) {
                    if (c.getConstraintType() != Constraint.Type.REFERENTIAL) {
                        continue;
                    }
                    ConstraintReferential ref = (ConstraintReferential) c;
                    if (ref.getRefTable() == this) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public ArrayList<Session> checkDeadlock(Session session, Session clash, Set<Session> visited) {
        // only one deadlock check at any given time
        synchronized (getClass()) {
            if (clash == null) {
                // verification is started
                clash = session;
                visited = new HashSet<>();
            } else if (clash == session) {
                // we found a cycle where this session is involved
                return new ArrayList<>(0);
            } else if (visited.contains(session)) {
                // we have already checked this session.
                // there is a cycle, but the sessions in the cycle need to
                // find it out themselves
                return null;
            }
            visited.add(session);
            ArrayList<Session> error = null;
            for (Session s : lockSharedSessions.keySet()) {
                if (s == session) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }
                Table t = s.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(s, clash, visited);
                    if (error != null) {
                        error.add(session);
                        break;
                    }
                }
            }
            // take a local copy so we don't see inconsistent data, since we are
            // not locked while checking the lockExclusiveSession value
            Session copyOfLockExclusiveSession = lockExclusiveSession;
            if (error == null && copyOfLockExclusiveSession != null) {
                Table t = copyOfLockExclusiveSession.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(copyOfLockExclusiveSession, clash, visited);
                    if (error != null) {
                        error.add(session);
                    }
                }
            }
            return error;
        }
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    public boolean getContainsLargeObject() {
        return containsLargeObject;
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, Value.LONG);
            rowIdColumn.setTable(this, SearchRow.ROWID_INDEX);
            rowIdColumn.setRowId(true);
        }
        return rowIdColumn;
    }

    @Override
    public TableType getTableType() {
        return TableType.TABLE;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean isLockedExclusively() {
        return lockExclusiveSession != null;
    }

    @Override
    public boolean isLockedExclusivelyBy(Session session) {
        return lockExclusiveSession == session;
    }

    @Override
    public String toString() {
        return getSQL(false);
    }

}
