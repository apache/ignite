/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * UPDATE
 */
public class Update extends Prepared {

    private Expression condition;
    private TableFilter targetTableFilter;// target of update
    /**
     * This table filter is for MERGE..USING support - not used in stand-alone DML
     */
    private TableFilter sourceTableFilter;

    /** The limit expression as specified in the LIMIT clause. */
    private Expression limitExpr;

    private boolean updateToCurrentValuesReturnsZero;

    private final ArrayList<Column> columns = Utils.newSmallArrayList();
    private final HashMap<Column, Expression> expressionMap  = new HashMap<>();

    private HashSet<Long> updatedKeysCollector;

    public Update(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.targetTableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition( ) {
        return this.condition;
    }

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column the column
     * @param expression the expression
     */
    public void setAssignment(Column column, Expression expression) {
        if (expressionMap.containsKey(column)) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column
                    .getName());
        }
        columns.add(column);
        expressionMap.put(column, expression);
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    /**
     * Sets the collector of updated keys.
     *
     * @param updatedKeysCollector the collector of updated keys
     */
    public void setUpdatedKeysCollector(HashSet<Long> updatedKeysCollector) {
        this.updatedKeysCollector = updatedKeysCollector;
    }

    @Override
    public int update() {
        targetTableFilter.startQuery(session);
        targetTableFilter.reset();
        try (RowList rows = new RowList(session)) {
            Table table = targetTableFilter.getTable();
            session.getUser().checkRight(table, Right.UPDATE);
            table.fire(session, Trigger.UPDATE, true);
            table.lock(session, true, false);
            int columnCount = table.getColumns().length;
            // get the old rows, compute the new rows
            setCurrentRowNumber(0);
            int count = 0;
            Column[] columns = table.getColumns();
            int limitRows = -1;
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            while (targetTableFilter.next()) {
                setCurrentRowNumber(count+1);
                if (limitRows >= 0 && count >= limitRows) {
                    break;
                }
                if (condition == null || condition.getBooleanValue(session)) {
                    Row oldRow = targetTableFilter.get();
                    if (table.isMVStore()) {
                        Row lockedRow = table.lockRow(session, oldRow);
                        if (lockedRow == null) {
                            continue;
                        }
                        if (!oldRow.hasSharedData(lockedRow)) {
                            oldRow = lockedRow;
                            targetTableFilter.set(oldRow);
                            if (condition != null && !condition.getBooleanValue(session)) {
                                continue;
                            }
                        }
                    }
                    Row newRow = table.getTemplateRow();
                    boolean setOnUpdate = false;
                    for (int i = 0; i < columnCount; i++) {
                        Expression newExpr = expressionMap.get(columns[i]);
                        Column column = table.getColumn(i);
                        Value newValue;
                        if (newExpr == null) {
                            if (column.getOnUpdateExpression() != null) {
                                setOnUpdate = true;
                            }
                            newValue = oldRow.getValue(i);
                        } else if (newExpr == ValueExpression.getDefault()) {
                            newValue = table.getDefaultValue(session, column);
                        } else {
                            newValue = column.convert(newExpr.getValue(session), session.getDatabase().getMode());
                        }
                        newRow.setValue(i, newValue);
                    }
                    long key = oldRow.getKey();
                    newRow.setKey(key);
                    if (setOnUpdate || updateToCurrentValuesReturnsZero) {
                        setOnUpdate = false;
                        for (int i = 0; i < columnCount; i++) {
                            // Use equals here to detect changes from numeric 0 to 0.0 and similar
                            if (!Objects.equals(oldRow.getValue(i), newRow.getValue(i))) {
                                setOnUpdate = true;
                                break;
                            }
                        }
                        if (setOnUpdate) {
                            for (int i = 0; i < columnCount; i++) {
                                if (expressionMap.get(columns[i]) == null) {
                                    Column column = table.getColumn(i);
                                    if (column.getOnUpdateExpression() != null) {
                                        newRow.setValue(i, table.getOnUpdateValue(session, column));
                                    }
                                }
                            }
                        } else if (updateToCurrentValuesReturnsZero) {
                            count--;
                        }
                    }
                    table.validateConvertUpdateSequence(session, newRow);
                    if (!table.fireRow() || !table.fireBeforeRow(session, oldRow, newRow)) {
                        rows.add(oldRow);
                        rows.add(newRow);
                        if (updatedKeysCollector != null) {
                            updatedKeysCollector.add(key);
                        }
                    }
                    count++;
                }
            }
            // TODO self referencing referential integrity constraints
            // don't work if update is multi-row and 'inversed' the condition!
            // probably need multi-row triggers with 'deleted' and 'inserted'
            // at the same time. anyway good for sql compatibility
            // TODO update in-place (but if the key changes,
            // we need to update all indexes) before row triggers

            // the cached row is already updated - we need the old values
            table.updateRows(this, session, rows);
            if (table.fireRow()) {
                for (rows.reset(); rows.hasNext();) {
                    Row o = rows.next();
                    Row n = rows.next();
                    table.fireAfterRow(session, o, n, false);
                }
            }
            table.fire(session, Trigger.UPDATE, false);
            return count;
        }
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder builder = new StringBuilder("UPDATE ");
        targetTableFilter.getPlanSQL(builder, false, alwaysQuote).append("\nSET\n    ");
        for (int i = 0, size = columns.size(); i < size; i++) {
            if (i > 0) {
                builder.append(",\n    ");
            }
            Column c = columns.get(i);
            c.getSQL(builder, alwaysQuote).append(" = ");
            expressionMap.get(c).getSQL(builder, alwaysQuote);
        }
        if (condition != null) {
            builder.append("\nWHERE ");
            condition.getUnenclosedSQL(builder, alwaysQuote);
        }
        if (limitExpr != null) {
            builder.append("\nLIMIT ");
            limitExpr.getUnenclosedSQL(builder, alwaysQuote);
        }
        return builder.toString();
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, targetTableFilter);
        }
        for (Column c : columns) {
            Expression e = expressionMap.get(c);
            e.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            if (sourceTableFilter!=null){
                e.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
            }
            expressionMap.put(c, e.optimize(session));
        }
        TableFilter[] filters;
        if(sourceTableFilter==null){
            filters = new TableFilter[] { targetTableFilter };
        }
        else{
            filters = new TableFilter[] { targetTableFilter, sourceTableFilter };
        }
        PlanItem item = targetTableFilter.getBestPlanItem(session, filters, 0,
                new AllColumnsForPlan(filters));
        targetTableFilter.setPlanItem(item);
        targetTableFilter.prepare();
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
        return CommandInterface.UPDATE;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public TableFilter getSourceTableFilter() {
        return sourceTableFilter;
    }

    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;
    }

    /**
     * Sets expected update count for update to current values case.
     *
     * @param updateToCurrentValuesReturnsZero if zero should be returned as update
     *        count if update set row to current values
     */
    public void setUpdateToCurrentValuesReturnsZero(boolean updateToCurrentValuesReturnsZero) {
        this.updateToCurrentValuesReturnsZero = updateToCurrentValuesReturnsZero;
    }
}
