/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.HashSet;

import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * DELETE
 */
public class Delete extends Prepared {

    private Expression condition;
    private TableFilter targetTableFilter;

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    private Expression limitExpr;
    /**
     * This table filter is for MERGE..USING support - not used in stand-alone DML
     */
    private TableFilter sourceTableFilter;

    private HashSet<Long> keysFilter;

    public Delete(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.targetTableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return this.condition;
    }

    /**
     * Sets the keys filter.
     *
     * @param keysFilter the keys filter
     */
    public void setKeysFilter(HashSet<Long> keysFilter) {
        this.keysFilter = keysFilter;
    }

    @Override
    public int update() {
        targetTableFilter.startQuery(session);
        targetTableFilter.reset();
        Table table = targetTableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fire(session, Trigger.DELETE, true);
        table.lock(session, true, false);
        int limitRows = -1;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                limitRows = v.getInt();
            }
        }
        try (RowList rows = new RowList(session)) {
            setCurrentRowNumber(0);
            int count = 0;
            while (limitRows != 0 && targetTableFilter.next()) {
                setCurrentRowNumber(rows.size() + 1);
                if (condition == null || condition.getBooleanValue(session)) {
                    Row row = targetTableFilter.get();
                    if (keysFilter == null || keysFilter.contains(row.getKey())) {
                        if (table.isMVStore()) {
                            Row lockedRow = table.lockRow(session, row);
                            if (lockedRow == null) {
                                continue;
                            }
                            if (!row.hasSharedData(lockedRow)) {
                                row = lockedRow;
                                targetTableFilter.set(row);
                                if (condition != null && !condition.getBooleanValue(session)) {
                                    continue;
                                }
                            }
                        }
                        if (!table.fireRow() || !table.fireBeforeRow(session, row, null)) {
                            rows.add(row);
                        }
                        count++;
                        if (limitRows >= 0 && count >= limitRows) {
                            break;
                        }
                    }
                }
            }
            int rowScanCount = 0;
            for (rows.reset(); rows.hasNext();) {
                if ((++rowScanCount & 127) == 0) {
                    checkCanceled();
                }
                Row row = rows.next();
                table.removeRow(session, row);
                session.log(table, UndoLogRecord.DELETE, row);
            }
            if (table.fireRow()) {
                for (rows.reset(); rows.hasNext();) {
                    Row row = rows.next();
                    table.fireAfterRow(session, row, null, false);
                }
            }
            table.fire(session, Trigger.DELETE, false);
            return count;
        }
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder buff = new StringBuilder();
        buff.append("DELETE FROM ");
        targetTableFilter.getPlanSQL(buff, false, alwaysQuote);
        if (condition != null) {
            buff.append("\nWHERE ");
            condition.getUnenclosedSQL(buff, alwaysQuote);
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT (");
            limitExpr.getUnenclosedSQL(buff, alwaysQuote).append(')');
        }
        return buff.toString();
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            if (sourceTableFilter != null) {
                condition.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
            }
            condition = condition.optimize(session);
            condition.createIndexConditions(session, targetTableFilter);
        }
        TableFilter[] filters;
        if (sourceTableFilter == null) {
            filters = new TableFilter[] { targetTableFilter };
        } else {
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
        return CommandInterface.DELETE;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;
    }

    public TableFilter getTableFilter() {
        return targetTableFilter;
    }

    public TableFilter getSourceTableFilter() {
        return sourceTableFilter;
    }

}
