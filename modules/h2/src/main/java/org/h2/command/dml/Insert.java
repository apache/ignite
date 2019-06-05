/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashMap;
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.GeneratedKeys;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.Parameter;
import org.h2.expression.SequenceValue;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVPrimaryIndex;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * INSERT
 */
public class Insert extends Prepared implements ResultTarget {

    private Table table;
    private Column[] columns;
    private final ArrayList<Expression[]> list = New.arrayList();
    private Query query;
    private boolean sortedInsertMode;
    private int rowNumber;
    private boolean insertFromSelect;
    /**
     * This table filter is for MERGE..USING support - not used in stand-alone DML
     */
    private TableFilter sourceTableFilter;

    /**
     * For MySQL-style INSERT ... ON DUPLICATE KEY UPDATE ....
     */
    private HashMap<Column, Expression> duplicateKeyAssignmentMap;

    /**
     * For MySQL-style INSERT IGNORE
     */
    private boolean ignore;

    public Insert(Session session) {
        super(session);
    }

    @Override
    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    /**
     * Sets MySQL-style INSERT IGNORE mode
     * @param ignore ignore errors
     */
    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    /**
     * Keep a collection of the columns to pass to update if a duplicate key
     * happens, for MySQL-style INSERT ... ON DUPLICATE KEY UPDATE ....
     *
     * @param column the column
     * @param expression the expression
     */
    public void addAssignmentForDuplicate(Column column, Expression expression) {
        if (duplicateKeyAssignmentMap == null) {
            duplicateKeyAssignmentMap = new HashMap<>();
        }
        if (duplicateKeyAssignmentMap.containsKey(column)) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1,
                    column.getName());
        }
        duplicateKeyAssignmentMap.put(column, expression);
    }

    /**
     * Add a row to this merge statement.
     *
     * @param expr the list of values
     */
    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    @Override
    public int update() {
        Index index = null;
        if (sortedInsertMode) {
            index = table.getScanIndex(session);
            index.setSortedInsertMode(true);
        }
        try {
            return insertRows();
        } finally {
            if (index != null) {
                index.setSortedInsertMode(false);
            }
        }
    }

    private int insertRows() {
        session.getUser().checkRight(table, Right.INSERT);
        setCurrentRowNumber(0);
        table.fire(session, Trigger.INSERT, true);
        rowNumber = 0;
        GeneratedKeys generatedKeys = session.getGeneratedKeys();
        generatedKeys.initialize(table);
        int listSize = list.size();
        if (listSize > 0) {
            int columnLen = columns.length;
            for (int x = 0; x < listSize; x++) {
                session.startStatementWithinTransaction();
                generatedKeys.nextRow();
                Row newRow = table.getTemplateRow();
                Expression[] expr = list.get(x);
                setCurrentRowNumber(x + 1);
                for (int i = 0; i < columnLen; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        e = e.optimize(session);
                        try {
                            Value v = c.convert(e.getValue(session), session.getDatabase().getMode());
                            newRow.setValue(index, v);
                            if (e instanceof SequenceValue) {
                                generatedKeys.add(c);
                            }
                        } catch (DbException ex) {
                            throw setRow(ex, x, getSQL(expr));
                        }
                    }
                }
                rowNumber++;
                table.validateConvertUpdateSequence(session, newRow);
                boolean done = table.fireBeforeRow(session, null, newRow);
                if (!done) {
                    table.lock(session, true, false);
                    try {
                        table.addRow(session, newRow);
                    } catch (DbException de) {
                        if (!handleOnDuplicate(de)) {
                            // INSERT IGNORE case
                            rowNumber--;
                            continue;
                        }
                    }
                    generatedKeys.confirmRow(newRow);
                    session.log(table, UndoLogRecord.INSERT, newRow);
                    table.fireAfterRow(session, null, newRow, false);
                }
            }
        } else {
            table.lock(session, true, false);
            if (insertFromSelect) {
                query.query(0, this);
            } else {
                ResultInterface rows = query.query(0);
                while (rows.next()) {
                    generatedKeys.nextRow();
                    Value[] r = rows.currentRow();
                    Row newRow = addRowImpl(r);
                    if (newRow != null) {
                        generatedKeys.confirmRow(newRow);
                    }
                }
                rows.close();
            }
        }
        table.fire(session, Trigger.INSERT, false);
        return rowNumber;
    }

    @Override
    public void addRow(Value[] values) {
        addRowImpl(values);
    }

    private Row addRowImpl(Value[] values) {
        Row newRow = table.getTemplateRow();
        setCurrentRowNumber(++rowNumber);
        for (int j = 0, len = columns.length; j < len; j++) {
            Column c = columns[j];
            int index = c.getColumnId();
            try {
                Value v = c.convert(values[j], session.getDatabase().getMode());
                newRow.setValue(index, v);
            } catch (DbException ex) {
                throw setRow(ex, rowNumber, getSQL(values));
            }
        }
        table.validateConvertUpdateSequence(session, newRow);
        boolean done = table.fireBeforeRow(session, null, newRow);
        if (!done) {
            table.addRow(session, newRow);
            session.log(table, UndoLogRecord.INSERT, newRow);
            table.fireAfterRow(session, null, newRow, false);
            return newRow;
        }
        return null;
    }

    @Override
    public int getRowCount() {
        return rowNumber;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(")\n");
        if (insertFromSelect) {
            buff.append("DIRECT ");
        }
        if (sortedInsertMode) {
            buff.append("SORTED ");
        }
        if (!list.isEmpty()) {
            buff.append("VALUES ");
            int row = 0;
            if (list.size() > 1) {
                buff.append('\n');
            }
            for (Expression[] expr : list) {
                if (row++ > 0) {
                    buff.append(",\n");
                }
                buff.append('(');
                buff.resetCount();
                for (Expression e : expr) {
                    buff.appendExceptFirst(", ");
                    if (e == null) {
                        buff.append("DEFAULT");
                    } else {
                        buff.append(e.getSQL());
                    }
                }
                buff.append(')');
            }
        } else {
            buff.append(query.getPlanSQL());
        }
        return buff.toString();
    }

    @Override
    public void prepare() {
        if (columns == null) {
            if (!list.isEmpty() && list.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (!list.isEmpty()) {
            for (Expression[] expr : list) {
                if (expr.length != columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0, len = expr.length; i < len; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        if(sourceTableFilter!=null){
                            e.mapColumns(sourceTableFilter, 0);
                        }
                        e = e.optimize(session);
                        if (e instanceof Parameter) {
                            Parameter p = (Parameter) e;
                            p.setColumn(columns[i]);
                        }
                        expr[i] = e;
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setSortedInsertMode(boolean sortedInsertMode) {
        this.sortedInsertMode = sortedInsertMode;
    }

    @Override
    public int getType() {
        return CommandInterface.INSERT;
    }

    public void setInsertFromSelect(boolean value) {
        this.insertFromSelect = value;
    }

    @Override
    public boolean isCacheable() {
        return duplicateKeyAssignmentMap == null ||
                duplicateKeyAssignmentMap.isEmpty();
    }

    /**
     * @param de duplicate key exception
     * @return {@code true} if row was updated, {@code false} if row was ignored
     */
    private boolean handleOnDuplicate(DbException de) {
        if (de.getErrorCode() != ErrorCode.DUPLICATE_KEY_1) {
            throw de;
        }
        if (duplicateKeyAssignmentMap == null ||
                duplicateKeyAssignmentMap.isEmpty()) {
            if (ignore) {
                return false;
            }
            throw de;
        }

        ArrayList<String> variableNames = new ArrayList<>(
                duplicateKeyAssignmentMap.size());
        Expression[] row = list.get(getCurrentRowNumber() - 1);
        for (int i = 0; i < columns.length; i++) {
            String key = table.getSchema().getName() + "." +
                    table.getName() + "." + columns[i].getName();
            variableNames.add(key);
            session.setVariable(key,
                    row[i].getValue(session));
        }

        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(table.getSQL()).append(" SET ");
        for (Column column : duplicateKeyAssignmentMap.keySet()) {
            buff.appendExceptFirst(", ");
            Expression ex = duplicateKeyAssignmentMap.get(column);
            buff.append(column.getSQL()).append("=").append(ex.getSQL());
        }
        buff.append(" WHERE ");
        Index foundIndex = (Index) de.getSource();
        if (foundIndex == null) {
            throw DbException.getUnsupportedException(
                    "Unable to apply ON DUPLICATE KEY UPDATE, no index found!");
        }
        buff.append(prepareUpdateCondition(foundIndex).getSQL());
        String sql = buff.toString();
        Prepared command = session.prepare(sql);
        for (Parameter param : command.getParameters()) {
            Parameter insertParam = parameters.get(param.getIndex());
            param.setValue(insertParam.getValue(session));
        }
        command.update();
        for (String variableName : variableNames) {
            session.setVariable(variableName, ValueNull.INSTANCE);
        }
        return true;
    }

    private Expression prepareUpdateCondition(Index foundIndex) {
        // MVPrimaryIndex is playing fast and loose with it's implementation of
        // the Index interface.
        // It returns all of the columns in the table when we call
        // getIndexColumns() or getColumns().
        // Don't have time right now to fix that, so just special-case it.
        final Column[] indexedColumns;
        if (foundIndex instanceof MVPrimaryIndex) {
            MVPrimaryIndex foundMV = (MVPrimaryIndex) foundIndex;
            indexedColumns = new Column[] { foundMV.getIndexColumns()[foundMV
                    .getMainIndexColumn()].column };
        } else {
            indexedColumns = foundIndex.getColumns();
        }

        Expression[] row = list.get(getCurrentRowNumber() - 1);
        Expression condition = null;
        for (Column column : indexedColumns) {
            ExpressionColumn expr = new ExpressionColumn(session.getDatabase(),
                    table.getSchema().getName(), table.getName(),
                    column.getName());
            for (int i = 0; i < columns.length; i++) {
                if (expr.getColumnName().equals(columns[i].getName())) {
                    if (condition == null) {
                        condition = new Comparison(session, Comparison.EQUAL, expr, row[i]);
                    } else {
                        condition = new ConditionAndOr(ConditionAndOr.AND, condition,
                                new Comparison(session, Comparison.EQUAL, expr, row[i]));
                    }
                    break;
                }
            }
        }
        return condition;
    }

    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;
    }

}
