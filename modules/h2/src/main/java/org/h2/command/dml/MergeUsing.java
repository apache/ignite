/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * This class represents the statement syntax
 * MERGE INTO table alias USING...
 *
 * It does not replace the MERGE INTO... KEYS... form.
 */
public class MergeUsing extends Prepared {

    /**
     * Abstract WHEN command of the MERGE statement.
     */
    public static abstract class When {

        /**
         * The parent MERGE statement.
         */
        final MergeUsing mergeUsing;

        /**
         * AND condition of the command.
         */
        Expression andCondition;

        When(MergeUsing mergeUsing) {
            this.mergeUsing = mergeUsing;
        }

        /**
         * Sets the specified AND condition.
         *
         * @param andCondition AND condition to set
         */
        public void setAndCondition(Expression andCondition) {
            this.andCondition = andCondition;
        }

        /**
         * Reset updated keys if needs.
         */
        void reset() {
            // Nothing to do
        }

        /**
         * Merges rows.
         *
         * @return count of updated rows.
         */
        abstract int merge();

        /**
         * Prepares WHEN command.
         */
        void prepare() {
            if (andCondition != null) {
                andCondition.mapColumns(mergeUsing.sourceTableFilter, 2, Expression.MAP_INITIAL);
                andCondition.mapColumns(mergeUsing.targetTableFilter, 1, Expression.MAP_INITIAL);
            }
        }

        /**
         * Evaluates trigger mask (UPDATE, INSERT, DELETE).
         *
         * @return the trigger mask.
         */
        abstract int evaluateTriggerMasks();

        /**
         * Checks user's INSERT, UPDATE, DELETE permission in appropriate cases.
         */
        abstract void checkRights();

    }

    public static final class WhenMatched extends When {

        private Update updateCommand;

        private Delete deleteCommand;

        private final HashSet<Long> updatedKeys = new HashSet<>();

        public WhenMatched(MergeUsing mergeUsing) {
            super(mergeUsing);
        }

        public Prepared getUpdateCommand() {
            return updateCommand;
        }

        public void setUpdateCommand(Update updateCommand) {
            this.updateCommand = updateCommand;
        }

        public Prepared getDeleteCommand() {
            return deleteCommand;
        }

        public void setDeleteCommand(Delete deleteCommand) {
            this.deleteCommand = deleteCommand;
        }

        @Override
        void reset() {
            updatedKeys.clear();
        }

        @Override
        int merge() {
            int countUpdatedRows = 0;
            if (updateCommand != null) {
                countUpdatedRows += updateCommand.update();
            }
            // under oracle rules these updates & delete combinations are
            // allowed together
            if (deleteCommand != null) {
                countUpdatedRows += deleteCommand.update();
                updatedKeys.clear();
            }
            return countUpdatedRows;
        }

        @Override
        void prepare() {
            super.prepare();
            if (updateCommand != null) {
                updateCommand.setSourceTableFilter(mergeUsing.sourceTableFilter);
                updateCommand.setCondition(appendCondition(updateCommand, mergeUsing.onCondition));
                if (andCondition != null) {
                    updateCommand.setCondition(appendCondition(updateCommand, andCondition));
                }
                updateCommand.prepare();
            }
            if (deleteCommand != null) {
                deleteCommand.setSourceTableFilter(mergeUsing.sourceTableFilter);
                deleteCommand.setCondition(appendCondition(deleteCommand, mergeUsing.onCondition));
                if (andCondition != null) {
                    deleteCommand.setCondition(appendCondition(deleteCommand, andCondition));
                }
                deleteCommand.prepare();
                if (updateCommand != null) {
                    updateCommand.setUpdatedKeysCollector(updatedKeys);
                    deleteCommand.setKeysFilter(updatedKeys);
                }
            }
        }

        @Override
        int evaluateTriggerMasks() {
            int masks = 0;
            if (updateCommand != null) {
                masks |= Trigger.UPDATE;
            }
            if (deleteCommand != null) {
                masks |= Trigger.DELETE;
            }
            return masks;
        }

        @Override
        void checkRights() {
            User user = mergeUsing.getSession().getUser();
            if (updateCommand != null) {
                user.checkRight(mergeUsing.targetTable, Right.UPDATE);
            }
            if (deleteCommand != null) {
                user.checkRight(mergeUsing.targetTable, Right.DELETE);
            }
        }

        private static Expression appendCondition(Update updateCommand, Expression condition) {
            Expression c = updateCommand.getCondition();
            return c == null ? condition : new ConditionAndOr(ConditionAndOr.AND, c, condition);
        }

        private static Expression appendCondition(Delete deleteCommand, Expression condition) {
            Expression c = deleteCommand.getCondition();
            return c == null ? condition : new ConditionAndOr(ConditionAndOr.AND, c, condition);
        }

    }

    public static final class WhenNotMatched extends When {

        private Insert insertCommand;

        public WhenNotMatched(MergeUsing mergeUsing) {
            super(mergeUsing);
        }

        public Insert getInsertCommand() {
            return insertCommand;
        }

        public void setInsertCommand(Insert insertCommand) {
            this.insertCommand = insertCommand;
        }

        @Override
        int merge() {
            return andCondition == null || andCondition.getBooleanValue(mergeUsing.getSession()) ?
                    insertCommand.update() : 0;
        }

        @Override
        void prepare() {
            super.prepare();
            insertCommand.setSourceTableFilter(mergeUsing.sourceTableFilter);
            insertCommand.prepare();
        }

        @Override
        int evaluateTriggerMasks() {
            return Trigger.INSERT;
        }

        @Override
        void checkRights() {
            mergeUsing.getSession().getUser().checkRight(mergeUsing.targetTable, Right.INSERT);
        }

    }

    // Merge fields
    /**
     * Target table.
     */
    Table targetTable;

    /**
     * Target table filter.
     */
    TableFilter targetTableFilter;

    private Query query;

    // MergeUsing fields
    /**
     * Source table filter.
     */
    TableFilter sourceTableFilter;

    /**
     * ON condition expression.
     */
    Expression onCondition;
    private ArrayList<When> when = Utils.newSmallArrayList();
    private String queryAlias;
    private int countUpdatedRows;
    private Select targetMatchQuery;
    /**
     * Contains mappings between _ROWID_ and ROW_NUMBER for processed rows. Row
     * identities are remembered to prevent duplicate updates of the same row.
     */
    private final HashMap<Value, Integer> targetRowidsRemembered = new HashMap<>();
    private int sourceQueryRowNumber;


    public MergeUsing(Session session, TableFilter targetTableFilter) {
        super(session);
        this.targetTable = targetTableFilter.getTable();
        this.targetTableFilter = targetTableFilter;
    }

    @Override
    public int update() {
        countUpdatedRows = 0;

        // clear list of source table keys & rowids we have processed already
        targetRowidsRemembered.clear();

        targetTableFilter.startQuery(session);
        targetTableFilter.reset();

        sourceTableFilter.startQuery(session);
        sourceTableFilter.reset();

        sourceQueryRowNumber = 0;
        checkRights();
        setCurrentRowNumber(0);
        for (When w : when) {
            w.reset();
        }
        // process source select query data for row creation
        ResultInterface rows = query.query(0);
        targetTable.fire(session, evaluateTriggerMasks(), true);
        targetTable.lock(session, true, false);
        while (rows.next()) {
            sourceQueryRowNumber++;
            Value[] sourceRowValues = rows.currentRow();
            Row sourceRow = new RowImpl(sourceRowValues, 0);
            setCurrentRowNumber(sourceQueryRowNumber);

            merge(sourceRow);
        }
        rows.close();
        targetTable.fire(session, evaluateTriggerMasks(), false);
        return countUpdatedRows;
    }

    private int evaluateTriggerMasks() {
        int masks = 0;
        for (When w : when) {
            masks |= w.evaluateTriggerMasks();
        }
        return masks;
    }

    private void checkRights() {
        for (When w : when) {
            w.checkRights();
        }
        // check the underlying tables
        session.getUser().checkRight(targetTable, Right.SELECT);
        session.getUser().checkRight(sourceTableFilter.getTable(), Right.SELECT);
    }

    /**
     * Merge the given row.
     *
     * @param sourceRow the row
     */
    protected void merge(Row sourceRow) {
        // put the column values into the table filter
        sourceTableFilter.set(sourceRow);
        boolean found = isTargetRowFound();
        for (When w : when) {
            if (w.getClass() == WhenNotMatched.class ^ found) {
                countUpdatedRows += w.merge();
            }
        }
    }

    private boolean isTargetRowFound() {
        boolean matched = false;
        try (ResultInterface rows = targetMatchQuery.query(0)) {
            while (rows.next()) {
                Value targetRowId = rows.currentRow()[0];
                Integer number = targetRowidsRemembered.get(targetRowId);
                // throw and exception if we have processed this _ROWID_ before...
                if (number != null) {
                    throw DbException.get(ErrorCode.DUPLICATE_KEY_1,
                            "Merge using ON column expression, " +
                            "duplicate _ROWID_ target record already updated, deleted or inserted:_ROWID_="
                                    + targetRowId + ":in:"
                                    + targetTableFilter.getTable()
                                    + ":conflicting source row number:"
                                    + number);
                }
                // remember the source column values we have used before (they
                // are the effective ON clause keys
                // and should not be repeated
                targetRowidsRemembered.put(targetRowId, sourceQueryRowNumber);
                matched = true;
            }
        }
        return matched;
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder builder = new StringBuilder("MERGE INTO ");
        targetTable.getSQL(builder, alwaysQuote).append('\n').append("USING ").append(query.getPlanSQL(alwaysQuote));
        // TODO add aliases and WHEN clauses to make plan SQL more like original SQL
        return builder.toString();
    }

    @Override
    public void prepare() {
        onCondition.addFilterConditions(sourceTableFilter, true);
        onCondition.addFilterConditions(targetTableFilter, true);

        onCondition.mapColumns(sourceTableFilter, 2, Expression.MAP_INITIAL);
        onCondition.mapColumns(targetTableFilter, 1, Expression.MAP_INITIAL);

        // only do the optimize now - before we have already gathered the
        // unoptimized column data
        onCondition = onCondition.optimize(session);
        onCondition.createIndexConditions(session, sourceTableFilter);
        onCondition.createIndexConditions(session, targetTableFilter);

        query.prepare();

        // Prepare each of the sub-commands ready to aid in the MERGE
        // collaboration
        targetTableFilter.doneWithIndexConditions();
        boolean forUpdate = false;
        for (When w : when) {
            w.prepare();
            if (w instanceof WhenNotMatched) {
                forUpdate = true;
            }
        }

        // setup the targetMatchQuery - for detecting if the target row exists
        targetMatchQuery = new Select(session, null);
        ArrayList<Expression> expressions = new ArrayList<>(1);
        expressions.add(new ExpressionColumn(session.getDatabase(), targetTable.getSchema().getName(),
                targetTableFilter.getTableAlias(), Column.ROWID, true));
        targetMatchQuery.setExpressions(expressions);
        targetMatchQuery.addTableFilter(targetTableFilter, true);
        targetMatchQuery.addCondition(onCondition);
        targetMatchQuery.setForUpdate(forUpdate);
        targetMatchQuery.init();
        targetMatchQuery.prepare();
    }

    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;
    }

    public TableFilter getSourceTableFilter() {
        return sourceTableFilter;
    }

    public void setOnCondition(Expression condition) {
        this.onCondition = condition;
    }

    public Expression getOnCondition() {
        return onCondition;
    }

    public ArrayList<When> getWhen() {
        return when;
    }

    /**
     * Adds WHEN command.
     *
     * @param w new WHEN command to add (update, delete or insert).
     */
    public void addWhen(When w) {
        when.add(w);
    }

    public void setQueryAlias(String alias) {
        this.queryAlias = alias;

    }

    public String getQueryAlias() {
        return this.queryAlias;

    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setTargetTableFilter(TableFilter targetTableFilter) {
        this.targetTableFilter = targetTableFilter;
    }

    public TableFilter getTargetTableFilter() {
        return targetTableFilter;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    // Prepared interface implementations

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

}
