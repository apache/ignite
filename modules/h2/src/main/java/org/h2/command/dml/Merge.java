/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.GeneratedKeys;
import org.h2.engine.Mode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVPrimaryIndex;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * This class represents the statement
 * MERGE
 */
public class Merge extends CommandWithValues {

    private Table targetTable;
    private TableFilter targetTableFilter;
    private Column[] columns;
    private Column[] keys;
    private Query query;
    private Prepared update;

    public Merge(Session session) {
        super(session);
    }

    @Override
    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setKeys(Column[] keys) {
        this.keys = keys;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public int update() {
        int count;
        session.getUser().checkRight(targetTable, Right.INSERT);
        session.getUser().checkRight(targetTable, Right.UPDATE);
        setCurrentRowNumber(0);
        GeneratedKeys generatedKeys = session.getGeneratedKeys();
        Mode mode = session.getDatabase().getMode();
        if (!valuesExpressionList.isEmpty()) {
            // process values in list
            count = 0;
            generatedKeys.initialize(targetTable);
            for (int x = 0, size = valuesExpressionList.size(); x < size; x++) {
                setCurrentRowNumber(x + 1);
                generatedKeys.nextRow();
                Expression[] expr = valuesExpressionList.get(x);
                Row newRow = targetTable.getTemplateRow();
                for (int i = 0, len = columns.length; i < len; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        try {
                            Value v = c.convert(e.getValue(session), mode);
                            newRow.setValue(index, v);
                            if (e.isGeneratedKey()) {
                                generatedKeys.add(c);
                            }
                        } catch (DbException ex) {
                            throw setRow(ex, count, getSimpleSQL(expr));
                        }
                    }
                }
                merge(newRow);
                count++;
            }
        } else {
            // process select data for list
            query.setNeverLazy(true);
            ResultInterface rows = query.query(0);
            count = 0;
            targetTable.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            targetTable.lock(session, true, false);
            while (rows.next()) {
                count++;
                generatedKeys.nextRow();
                Value[] r = rows.currentRow();
                Row newRow = targetTable.getTemplateRow();
                setCurrentRowNumber(count);
                for (int j = 0; j < columns.length; j++) {
                    Column c = columns[j];
                    int index = c.getColumnId();
                    try {
                        Value v = c.convert(r[j], mode);
                        newRow.setValue(index, v);
                    } catch (DbException ex) {
                        throw setRow(ex, count, getSQL(r));
                    }
                }
                merge(newRow);
            }
            rows.close();
            targetTable.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        }
        return count;
    }

    /**
     * Merge the given row.
     *
     * @param row the row
     */
    protected void merge(Row row) {
        ArrayList<Parameter> k = update.getParameters();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            Value v = row.getValue(col.getColumnId());
            Parameter p = k.get(i);
            p.setValue(v);
        }
        for (int i = 0; i < keys.length; i++) {
            Column col = keys[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null) {
                throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL(false));
            }
            Parameter p = k.get(columns.length + i);
            p.setValue(v);
        }

        // try an update
        int count = update.update();

        // if update fails try an insert
        if (count == 0) {
            try {
                targetTable.validateConvertUpdateSequence(session, row);
                boolean done = targetTable.fireBeforeRow(session, null, row);
                if (!done) {
                    targetTable.lock(session, true, false);
                    targetTable.addRow(session, row);
                    session.getGeneratedKeys().confirmRow(row);
                    session.log(targetTable, UndoLogRecord.INSERT, row);
                    targetTable.fireAfterRow(session, null, row, false);
                }
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // possibly a concurrent merge or insert
                    Index index = (Index) e.getSource();
                    if (index != null) {
                        // verify the index columns match the key
                        Column[] indexColumns;
                        if (index instanceof MVPrimaryIndex) {
                            MVPrimaryIndex foundMV = (MVPrimaryIndex) index;
                            indexColumns = new Column[] {
                                    foundMV.getIndexColumns()[foundMV.getMainIndexColumn()].column };
                        } else {
                            indexColumns = index.getColumns();
                        }
                        boolean indexMatchesKeys;
                        if (indexColumns.length <= keys.length) {
                            indexMatchesKeys = true;
                            for (int i = 0; i < indexColumns.length; i++) {
                                if (indexColumns[i] != keys[i]) {
                                    indexMatchesKeys = false;
                                    break;
                                }
                            }
                        } else {
                            indexMatchesKeys = false;
                        }
                        if (indexMatchesKeys) {
                            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, targetTable.getName());
                        }
                    }
                }
                throw e;
            }
        } else if (count != 1) {
            throw DbException.get(ErrorCode.DUPLICATE_KEY_1, targetTable.getSQL(false));
        }
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder builder = new StringBuilder("MERGE INTO ");
        targetTable.getSQL(builder, alwaysQuote).append('(');
        Column.writeColumns(builder, columns, alwaysQuote);
        builder.append(')');
        if (keys != null) {
            builder.append(" KEY(");
            Column.writeColumns(builder, keys, alwaysQuote);
            builder.append(')');
        }
        builder.append('\n');
        if (!valuesExpressionList.isEmpty()) {
            builder.append("VALUES ");
            int row = 0;
            for (Expression[] expr : valuesExpressionList) {
                if (row++ > 0) {
                    builder.append(", ");
                }
                builder.append('(');
                Expression.writeExpressions(builder, expr, alwaysQuote);
                builder.append(')');
            }
        } else {
            builder.append(query.getPlanSQL(alwaysQuote));
        }
        return builder.toString();
    }

    @Override
    public void prepare() {
        if (columns == null) {
            if (!valuesExpressionList.isEmpty() && valuesExpressionList.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = targetTable.getColumns();
            }
        }
        if (!valuesExpressionList.isEmpty()) {
            for (Expression[] expr : valuesExpressionList) {
                if (expr.length != columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0; i < expr.length; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        expr[i] = e.optimize(session);
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        if (keys == null) {
            Index idx = targetTable.getPrimaryKey();
            if (idx == null) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY");
            }
            keys = idx.getColumns();
        }
        StringBuilder builder = new StringBuilder("UPDATE ");
        targetTable.getSQL(builder, true).append(" SET ");
        Column.writeColumns(builder, columns, ", ", "=?", true).append(" WHERE ");
        Column.writeColumns(builder, keys, " AND ", "=?", true);
        update = session.prepare(builder.toString());
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.MERGE;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public TableFilter getTargetTableFilter() {
        return targetTableFilter;
    }

    public void setTargetTableFilter(TableFilter targetTableFilter) {
        this.targetTableFilter = targetTableFilter;
        setTargetTable(targetTableFilter.getTable());
    }



}
