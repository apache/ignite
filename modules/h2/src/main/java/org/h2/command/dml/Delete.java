/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
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

    @Override
    public int update() {
        targetTableFilter.startQuery(session);
        targetTableFilter.reset();
        Table table = targetTableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fire(session, Trigger.DELETE, true);
        table.lock(session, true, false);
        RowList rows = new RowList(session);
        int limitRows = -1;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                limitRows = v.getInt();
            }
        }
        try {
            setCurrentRowNumber(0);
            int count = 0;
            while (limitRows != 0 && targetTableFilter.next()) {
                setCurrentRowNumber(rows.size() + 1);
                if (condition == null || condition.getBooleanValue(session)) {
                    Row row = targetTableFilter.get();
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, row, null);
                    }
                    if (!done) {
                        rows.add(row);
                    }
                    count++;
                    if (limitRows >= 0 && count >= limitRows) {
                        break;
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
        } finally {
            rows.close();
        }
    }

    @Override
    public String getPlanSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append("DELETE ");
        buff.append("FROM ").append(targetTableFilter.getPlanSQL(false));
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(
                    condition.getSQL()));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT (").append(StringUtils.unEnclose(
                    limitExpr.getSQL())).append(')');
        }
        return buff.toString();
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(targetTableFilter, 0);
            if (sourceTableFilter != null) {
                condition.mapColumns(sourceTableFilter, 0);
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
                ExpressionVisitor.allColumnsForTableFilters(filters));
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
