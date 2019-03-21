/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.table.TableLink;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A linked index is a index for a linked (remote) table.
 * It is backed by an index on the remote table which is accessed over JDBC.
 */
public class LinkedIndex extends BaseIndex {

    private final TableLink link;
    private final String targetTableName;
    private long rowCount;

    private final boolean quoteAllIdentifiers = false;

    public LinkedIndex(TableLink table, int id, IndexColumn[] columns,
            IndexType indexType) {
        super(table, id, null, columns, indexType);
        link = table;
        targetTableName = link.getQualifiedTable();
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    private static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    @Override
    public void add(Session session, Row row) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder buff = new StringBuilder("INSERT INTO ");
        buff.append(targetTableName).append(" VALUES(");
        for (int i = 0; i < row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            if (i > 0) {
                buff.append(", ");
            }
            if (v == null) {
                buff.append("DEFAULT");
            } else if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
                params.add(v);
            }
        }
        buff.append(')');
        String sql = buff.toString();
        try {
            link.execute(sql, params, true);
            rowCount++;
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder builder = new StringBuilder("SELECT * FROM ").append(targetTableName).append(" T");
        boolean f = false;
        for (int i = 0; first != null && i < first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if (v != null) {
                builder.append(f ? " AND " : " WHERE ");
                f = true;
                Column col = table.getColumn(i);
                col.getSQL(builder, quoteAllIdentifiers);
                if (v == ValueNull.INSTANCE) {
                    builder.append(" IS NULL");
                } else {
                    builder.append(">=");
                    addParameter(builder, col);
                    params.add(v);
                }
            }
        }
        for (int i = 0; last != null && i < last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if (v != null) {
                builder.append(f ? " AND " : " WHERE ");
                f = true;
                Column col = table.getColumn(i);
                col.getSQL(builder, quoteAllIdentifiers);
                if (v == ValueNull.INSTANCE) {
                    builder.append(" IS NULL");
                } else {
                    builder.append("<=");
                    addParameter(builder, col);
                    params.add(v);
                }
            }
        }
        String sql = builder.toString();
        try {
            PreparedStatement prep = link.execute(sql, params, false);
            ResultSet rs = prep.getResultSet();
            return new LinkedCursor(link, rs, session, sql, prep);
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    private void addParameter(StringBuilder builder, Column col) {
        TypeInfo type = col.getType();
        if (type.getValueType() == Value.STRING_FIXED && link.isOracle()) {
            // workaround for Oracle
            // create table test(id int primary key, name char(15));
            // insert into test values(1, 'Hello')
            // select * from test where name = ? -- where ? = "Hello" > no rows
            builder.append("CAST(? AS CHAR(").append(type.getPrecision()).append("))");
        } else {
            builder.append('?');
        }
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return 100 + getCostRangeIndex(masks, rowCount +
                Constants.COST_ROW_OFFSET, filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public void truncate(Session session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        // TODO optimization: could get the first or last value (in any case;
        // maybe not optimized)
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public void remove(Session session, Row row) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder builder = new StringBuilder("DELETE FROM ").append(targetTableName).append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            if (i > 0) {
                builder.append("AND ");
            }
            Column col = table.getColumn(i);
            col.getSQL(builder, quoteAllIdentifiers);
            Value v = row.getValue(i);
            if (isNull(v)) {
                builder.append(" IS NULL ");
            } else {
                builder.append('=');
                addParameter(builder, col);
                params.add(v);
                builder.append(' ');
            }
        }
        String sql = builder.toString();
        try {
            PreparedStatement prep = link.execute(sql, params, false);
            int count = prep.executeUpdate();
            link.reusePreparedStatement(prep, sql);
            rowCount -= count;
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    /**
     * Update a row using a UPDATE statement. This method is to be called if the
     * emit updates option is enabled.
     *
     * @param oldRow the old data
     * @param newRow the new data
     */
    public void update(Row oldRow, Row newRow) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder builder = new StringBuilder("UPDATE ").append(targetTableName).append(" SET ");
        for (int i = 0; i < newRow.getColumnCount(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            table.getColumn(i).getSQL(builder, quoteAllIdentifiers).append('=');
            Value v = newRow.getValue(i);
            if (v == null) {
                builder.append("DEFAULT");
            } else {
                builder.append('?');
                params.add(v);
            }
        }
        builder.append(" WHERE ");
        for (int i = 0; i < oldRow.getColumnCount(); i++) {
            Column col = table.getColumn(i);
            if (i > 0) {
                builder.append(" AND ");
            }
            col.getSQL(builder, quoteAllIdentifiers);
            Value v = oldRow.getValue(i);
            if (isNull(v)) {
                builder.append(" IS NULL");
            } else {
                builder.append('=');
                params.add(v);
                addParameter(builder, col);
            }
        }
        String sql = builder.toString();
        try {
            link.execute(sql, params, true);
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }
}
