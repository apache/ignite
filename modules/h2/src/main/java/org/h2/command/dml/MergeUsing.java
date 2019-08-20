/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

/**
 * This class represents the statement syntax
 * MERGE table alias USING...
 *
 * It does not replace the existing MERGE INTO... KEYS... form.
 *
 * It supports the SQL 2003/2008 standard MERGE  statement:
 *      http://en.wikipedia.org/wiki/Merge_%28SQL%29
 *
 * Database management systems Oracle Database, DB2, Teradata, EXASOL, Firebird, CUBRID, HSQLDB,
 * MS SQL, Vectorwise and Apache Derby & Postgres support the standard syntax of the
 * SQL 2003/2008 MERGE command:
 *
 *  MERGE INTO targetTable AS T USING sourceTable AS S ON (T.ID = S.ID)
 *    WHEN MATCHED THEN
 *        UPDATE SET column1 = value1 [, column2 = value2 ...] WHERE column1=valueUpdate
 *        DELETE WHERE column1=valueDelete
 *    WHEN NOT MATCHED THEN
 *        INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...]);
 *
 * Only Oracle support the additional optional DELETE clause.
 *
 * Implementation notes:
 *
 * 1) The ON clause must specify 1 or more columns from the TARGET table because they are
 *    used in the plan SQL WHERE statement. Otherwise an exception is raised.
 *
 * 2) The ON clause must specify 1 or more columns from the SOURCE table/query because they
 *    are used to track the join key values for every source table row - to prevent any
 *    TARGET rows from being updated twice per MERGE USING statement.
 *
 *    This is to implement a requirement from the MERGE INTO specification
 *    requiring each row from being updated more than once per MERGE USING statement.
 *    The source columns are used to gather the effective "key" values which have been
 *    updated, in order to implement this requirement.
 *    If the no SOURCE table/query columns are found in the ON clause, then an exception is
 *    raised.
 *
 *    The update row counts of the embedded UPDATE and DELETE statements are also tracked to
 *    ensure no more than 1 row is ever updated. (Note One special case of this is that
 *    the DELETE is allowed to affect the same row which was updated by UPDATE - this is an
 *    Oracle only extension.)
 *
 * 3) UPDATE and DELETE statements are allowed to specify extra conditional criteria
 *    (in the WHERE clause) to allow fine-grained control of actions when a record is found.
 *    The ON clause conditions are always prepended to the WHERE clause of these embedded
 *    statements, so they will never update more than the ON join condition.
 *
 * 4) Previously if neither UPDATE or DELETE clause is supplied, but INSERT is supplied - the INSERT
 *    action is always triggered. This is because the embedded UPDATE and DELETE statement's
 *    returned update row count is used to detect a matching join.
 *    If neither of the two the statements are provided, no matching join is NEVER detected.
 *
 *    A fix for this is now implemented as described below:
 *    We now generate a "matchSelect" query and use that to always detect
 *    a match join - rather than relying on UPDATE or DELETE statements.
 *
 *    This is an improvement, especially in the case that if either of the
 *    UPDATE or DELETE statements had their own fine-grained WHERE conditions, making
 *    them completely different conditions than the plain ON condition clause which
 *    the SQL author would be specifying/expecting.
 *
 *    An additional benefit of this solution is that this "matchSelect" query
 *    is used to return the ROWID of the found (or inserted) query - for more accurate
 *    enforcing of the only-update-each-target-row-once rule.
 */
public class MergeUsing extends Prepared {

    // Merge fields
    private Table targetTable;
    private TableFilter targetTableFilter;
    private Column[] columns;
    private Column[] keys;
    private final ArrayList<Expression[]> valuesExpressionList = New
            .arrayList();
    private Query query;

    // MergeUsing fields
    private TableFilter sourceTableFilter;
    private Expression onCondition;
    private Update updateCommand;
    private Delete deleteCommand;
    private Insert insertCommand;
    private String queryAlias;
    private int countUpdatedRows;
    private Column[] sourceKeys;
    private Select targetMatchQuery;
    private final HashMap<Value, Integer> targetRowidsRemembered = new HashMap<>();
    private int sourceQueryRowNumber;


    public MergeUsing(Merge merge) {
        super(merge.getSession());

        // bring across only the already parsed data from Merge...
        this.targetTable = merge.getTargetTable();
        this.targetTableFilter = merge.getTargetTableFilter();
    }

    @Override
    public int update() {

        // clear list of source table keys & rowids we have processed already
        targetRowidsRemembered.clear();

        if (targetTableFilter != null) {
            targetTableFilter.startQuery(session);
            targetTableFilter.reset();
        }

        if (sourceTableFilter != null) {
            sourceTableFilter.startQuery(session);
            sourceTableFilter.reset();
        }

        sourceQueryRowNumber = 0;
        checkRights();
        setCurrentRowNumber(0);

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
        if (insertCommand != null) {
            masks |= Trigger.INSERT;
        }
        if (updateCommand != null) {
            masks |= Trigger.UPDATE;
        }
        if (deleteCommand != null) {
            masks |= Trigger.DELETE;
        }
        return masks;
    }

    private void checkRights() {
        if (insertCommand != null) {
            session.getUser().checkRight(targetTable, Right.INSERT);
        }
        if (updateCommand != null) {
            session.getUser().checkRight(targetTable, Right.UPDATE);
        }
        if (deleteCommand != null) {
            session.getUser().checkRight(targetTable, Right.DELETE);
        }

        // check the underlying tables
        session.getUser().checkRight(targetTable, Right.SELECT);
        session.getUser().checkRight(sourceTableFilter.getTable(),
                Right.SELECT);
    }

    /**
     * Merge the given row.
     *
     * @param sourceRow the row
     */
    protected void merge(Row sourceRow) {
        // put the column values into the table filter
        sourceTableFilter.set(sourceRow);

        // Is the target row there already ?
        boolean rowFound = isTargetRowFound();

        // try and perform an update
        int rowUpdateCount = 0;

        if (rowFound) {
            if (updateCommand != null) {
                rowUpdateCount += updateCommand.update();
            }
            if (deleteCommand != null) {
                int deleteRowUpdateCount = deleteCommand.update();
                // under oracle rules these updates & delete combinations are
                // allowed together
                if (rowUpdateCount == 1 && deleteRowUpdateCount == 1) {
                    countUpdatedRows += deleteRowUpdateCount;
                    deleteRowUpdateCount = 0;
                } else {
                    rowUpdateCount += deleteRowUpdateCount;
                }
            }
        } else {
            // if either updates do nothing, try an insert
            if (rowUpdateCount == 0) {
                rowUpdateCount += addRowByCommandInsert(sourceRow);
            } else if (rowUpdateCount != 1) {
                throw DbException.get(ErrorCode.DUPLICATE_KEY_1,
                        "Duplicate key inserted " + rowUpdateCount
                                + " rows at once, only 1 expected:"
                                + targetTable.getSQL());
            }

        }
        countUpdatedRows += rowUpdateCount;
    }

    private boolean isTargetRowFound() {
        ResultInterface rows = targetMatchQuery.query(0);
        int countTargetRowsFound = 0;
        Value[] targetRowIdValue = null;

        while (rows.next()) {
            countTargetRowsFound++;
            targetRowIdValue = rows.currentRow();

            // throw and exception if we have processed this _ROWID_ before...
            if (targetRowidsRemembered.containsKey(targetRowIdValue[0])) {
                throw DbException.get(ErrorCode.DUPLICATE_KEY_1,
                        "Merge using ON column expression, " +
                        "duplicate _ROWID_ target record already updated, deleted or inserted:_ROWID_="
                                + targetRowIdValue[0].toString() + ":in:"
                                + targetTableFilter.getTable()
                                + ":conflicting source row number:"
                                + targetRowidsRemembered
                                        .get(targetRowIdValue[0]));
            } else {
                // remember the source column values we have used before (they
                // are the effective ON clause keys
                // and should not be repeated
                targetRowidsRemembered.put(targetRowIdValue[0],
                        sourceQueryRowNumber);
            }
        }
        rows.close();
        if (countTargetRowsFound > 1) {
            throw DbException.get(ErrorCode.DUPLICATE_KEY_1,
                    "Duplicate key updated " + countTargetRowsFound
                            + " rows at once, only 1 expected:_ROWID_="
                            + targetRowIdValue[0].toString() + ":in:"
                            + targetTableFilter.getTable()
                            + ":conflicting source row number:"
                            + targetRowidsRemembered.get(targetRowIdValue[0]));

        }
        return countTargetRowsFound > 0;
    }

    private int addRowByCommandInsert(Row sourceRow) {
        int localCount = 0;
        if (insertCommand != null) {
            localCount += insertCommand.update();
            if (!isTargetRowFound()) {
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "Expected to find key after row inserted, but none found. Insert does not match ON condition.:"
                                + targetTable.getSQL() + ":source row="
                                + Arrays.asList(sourceRow.getValueList()));
            }
        }
        return localCount;
    }

    // Use the regular merge syntax as our plan SQL
    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("MERGE INTO ");
        buff.append(targetTable.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');
        if (keys != null) {
            buff.append(" KEY(");
            buff.resetCount();
            for (Column c : keys) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');
        }
        buff.append('\n');
        if (!valuesExpressionList.isEmpty()) {
            buff.append("VALUES ");
            int row = 0;
            for (Expression[] expr : valuesExpressionList) {
                if (row++ > 0) {
                    buff.append(", ");
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
        onCondition.addFilterConditions(sourceTableFilter, true);
        onCondition.addFilterConditions(targetTableFilter, true);

        onCondition.mapColumns(sourceTableFilter, 2);
        onCondition.mapColumns(targetTableFilter, 1);

        if (keys == null) {
            HashSet<Column> targetColumns = buildColumnListFromOnCondition(
                    targetTableFilter);
            keys = targetColumns.toArray(new Column[0]);
        }
        if (keys.length == 0) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1,
                    "No references to target columns found in ON clause:"
                            + targetTableFilter.toString());
        }
        if (sourceKeys == null) {
            HashSet<Column> sourceColumns = buildColumnListFromOnCondition(
                    sourceTableFilter);
            sourceKeys = sourceColumns.toArray(new Column[0]);
        }
        if (sourceKeys.length == 0) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1,
                    "No references to source columns found in ON clause:"
                            + sourceTableFilter.toString());
        }

        // only do the optimize now - before we have already gathered the
        // unoptimized column data
        onCondition = onCondition.optimize(session);
        onCondition.createIndexConditions(session, sourceTableFilter);
        onCondition.createIndexConditions(session, targetTableFilter);

        if (columns == null) {
            if (!valuesExpressionList.isEmpty()
                    && valuesExpressionList.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = targetTable.getColumns();
            }
        }
        if (!valuesExpressionList.isEmpty()) {
            for (Expression[] expr : valuesExpressionList) {
                if (expr.length != columns.length) {
                    throw DbException
                            .get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
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

        int embeddedStatementsCount = 0;

        // Prepare each of the sub-commands ready to aid in the MERGE
        // collaboration
        if (updateCommand != null) {
            updateCommand.setSourceTableFilter(sourceTableFilter);
            updateCommand.setCondition(appendOnCondition(updateCommand));
            updateCommand.prepare();
            embeddedStatementsCount++;
        }
        if (deleteCommand != null) {
            deleteCommand.setSourceTableFilter(sourceTableFilter);
            deleteCommand.setCondition(appendOnCondition(deleteCommand));
            deleteCommand.prepare();
            embeddedStatementsCount++;
        }
        if (insertCommand != null) {
            insertCommand.setSourceTableFilter(sourceTableFilter);
            insertCommand.prepare();
            embeddedStatementsCount++;
        }

        if (embeddedStatementsCount == 0) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                    "At least UPDATE, DELETE or INSERT embedded statement must be supplied.");
        }

        // setup the targetMatchQuery - for detecting if the target row exists
        Expression targetMatchCondition = targetMatchQuery.getCondition();
        targetMatchCondition.addFilterConditions(sourceTableFilter, true);
        targetMatchCondition.mapColumns(sourceTableFilter, 2);
        targetMatchCondition = targetMatchCondition.optimize(session);
        targetMatchCondition.createIndexConditions(session, sourceTableFilter);
        targetMatchQuery.prepare();
    }

    private HashSet<Column> buildColumnListFromOnCondition(
            TableFilter anyTableFilter) {
        HashSet<Column> filteredColumns = new HashSet<>();
        HashSet<Column> columns = new HashSet<>();
        ExpressionVisitor visitor = ExpressionVisitor
                .getColumnsVisitor(columns);
        onCondition.isEverything(visitor);
        for (Column c : columns) {
            if (c != null && c.getTable() == anyTableFilter.getTable()) {
                filteredColumns.add(c);
            }
        }
        return filteredColumns;
    }

    private Expression appendOnCondition(Update updateCommand) {
        if (updateCommand.getCondition() == null) {
            return onCondition;
        }
        return new ConditionAndOr(ConditionAndOr.AND,
                updateCommand.getCondition(), onCondition);
    }

    private Expression appendOnCondition(Delete deleteCommand) {
        if (deleteCommand.getCondition() == null) {
            return onCondition;
        }
        return new ConditionAndOr(ConditionAndOr.AND,
                deleteCommand.getCondition(), onCondition);
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

    public Insert getInsertCommand() {
        return insertCommand;
    }

    public void setInsertCommand(Insert insertCommand) {
        this.insertCommand = insertCommand;
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

    public Select getTargetMatchQuery() {
        return targetMatchQuery;
    }

    public void setTargetMatchQuery(Select targetMatchQuery) {
        this.targetMatchQuery = targetMatchQuery;
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
