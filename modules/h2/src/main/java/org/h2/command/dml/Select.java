/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.expression.*;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.*;
import org.h2.table.*;
import org.h2.util.*;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 * This class represents a simple SELECT statement.
 *
 * For each select statement,
 * visibleColumnCount &lt;= distinctColumnCount &lt;= expressionCount.
 * The expression list count could include ORDER BY and GROUP BY expressions
 * that are not in the select list.
 *
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 *
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {

    /**
     * The main (top) table filter.
     */
    TableFilter topTableFilter;

    private final ArrayList<TableFilter> filters = New.arrayList();
    private final ArrayList<TableFilter> topFilters = New.arrayList();

    /**
     * The column list, including synthetic columns (columns not shown in the
     * result).
     */
    ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private Expression having;
    private Expression condition;

    /**
     * The visible columns (the ones required in the result).
     */
    int visibleColumnCount;

    private int distinctColumnCount;
    private ArrayList<SelectOrderBy> orderList;
    private ArrayList<Expression> group;

    /**
     * The indexes of the group-by columns.
     */
    int[] groupIndex;

    /**
     * Whether a column in the expression list is part of a group-by.
     */
    boolean[] groupByExpression;

    /**
     * The current group-by values.
     */
    HashMap<Expression, Object> currentGroup;

    private int havingIndex;
    private boolean isGroupQuery, isGroupSortedQuery;
    private boolean isForUpdate, isForUpdateMvcc;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery;
    private boolean isPrepared, checkInit;
    private boolean sortUsingIndex;
    private SortOrder sort;

    /**
     * The id of the current group.
     */
    int currentGroupRowId;

    public Select(Session session) {
        super(session);
    }

    @Override
    public boolean isUnion() {
        return false;
    }

    /**
     * Add a table to the query.
     *
     * @param filter the table to add
     * @param isTop if the table can be the first table in the query plan
     */
    public void addTableFilter(TableFilter filter, boolean isTop) {
        // Oracle doesn't check on duplicate aliases
        // String alias = filter.getAlias();
        // if (filterNames.contains(alias)) {
        //     throw Message.getSQLException(
        //         ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    public ArrayList<TableFilter> getTopFilters() {
        return topFilters;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressions = expressions;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public ArrayList<Expression> getGroupBy() {
        return group;
    }

    public HashMap<Expression, Object> getCurrentGroup() {
        return currentGroup;
    }

    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    @Override
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    @Override
    public boolean hasOrder() {
        return orderList != null || sort != null;
    }

    /**
     * Add a condition to the list of conditions.
     *
     * @param cond the condition to add
     */
    public void addCondition(Expression cond) {
        if (condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    public Expression getCondition() {
        return condition;
    }

    private LazyResult queryGroupSorted(int columnCount, ResultTarget result) {
        LazyResultGroupSorted lazyResult = new LazyResultGroupSorted(expressionArray, columnCount);
        if (result == null) {
            return lazyResult;
        }
        while (lazyResult.next()) {
            result.addRow(lazyResult.currentRow());
        }
        return null;
    }

    /**
     * Create a row with the current values, for queries with group-sort.
     *
     * @param keyValues the key values
     * @param columnCount the number of columns
     * @return the row
     */
    Value[] createGroupSortedRow(Value[] keyValues, int columnCount) {
        Value[] row = new Value[columnCount];
        for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
            row[groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (groupByExpression != null && groupByExpression[j]) {
                continue;
            }
            Expression expr = expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (isHavingNullOrFalse(row)) {
            return null;
        }
        row = keepOnlyDistinct(row, columnCount);
        return row;
    }

    private Value[] keepOnlyDistinct(Value[] row, int columnCount) {
        if (columnCount == distinctColumnCount) {
            return row;
        }
        // remove columns so that 'distinct' can filter duplicate rows
        return Arrays.copyOf(row, distinctColumnCount);
    }

    private boolean isHavingNullOrFalse(Value[] row) {
        return havingIndex >= 0 && !row[havingIndex].getBoolean();
    }

    private Index getGroupSortedIndex() {
        if (groupIndex == null || groupByExpression == null) {
            return null;
        }
        ArrayList<Index> indexes = topTableFilter.getTable().getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getIndexType().isScan()) {
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    // does not allow scanning entries
                    continue;
                }
                if (isGroupSortedIndex(topTableFilter, index)) {
                    return index;
                }
            }
        }
        return null;
    }

    private boolean isGroupSortedIndex(TableFilter tableFilter, Index index) {
        // check that all the GROUP BY expressions are part of the index
        Column[] indexColumns = index.getColumns();
        // also check that the first columns in the index are grouped
        boolean[] grouped = new boolean[indexColumns.length];
        outerLoop:
        for (int i = 0, size = expressions.size(); i < size; i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = expressions.get(i).getNonAliasExpression();
            if (!(expr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (tableFilter == exprCol.getTableFilter()) {
                    if (indexColumns[j].equals(exprCol.getColumn())) {
                        grouped[j] = true;
                        continue outerLoop;
                    }
                }
            }
            // We didn't find a matching index column
            // for one group by expression
            return false;
        }
        // check that the first columns in the index are grouped
        // good: index(a, b, c); group by b, a
        // bad: index(a, b, c); group by a, c
        for (int i = 1; i < grouped.length; i++) {
            if (!grouped[i - 1] && grouped[i]) {
                return false;
            }
        }
        return true;
    }

    private int getGroupByExpressionCount() {
        if (groupByExpression == null) {
            return 0;
        }
        int count = 0;
        for (boolean b : groupByExpression) {
            if (b) {
                ++count;
            }
        }
        return count;
    }

    boolean isConditionMet() {
        return condition == null || condition.getBooleanValue(session);
    }

    private void queryGroup(int columnCount, LocalResult result) {
        ValueHashMap<HashMap<Expression, Object>> groups =
                ValueHashMap.newInstance();
        int rowNumber = 0;
        setCurrentRowNumber(0);
        currentGroup = null;
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (isConditionMet()) {
                Value key;
                rowNumber++;
                if (groupIndex == null) {
                    key = defaultGroup;
                } else {
                    Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap<Expression, Object> values = groups.get(key);
                if (values == null) {
                    values = new HashMap<>();
                    groups.put(key, values);
                }
                currentGroup = values;
                currentGroupRowId++;
                for (int i = 0; i < columnCount; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row)) {
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
    }

    /**
     * Get the index that matches the ORDER BY list, if one exists. This is to
     * avoid running a separate ORDER BY if an index can be used. This is
     * specially important for large result sets, if only the first few rows are
     * important (LIMIT is used)
     *
     * @return the index if one is found
     */
    private Index getSortIndex() {
        if (sort == null) {
            return null;
        }
        ArrayList<Column> sortColumns = New.arrayList();
        for (int idx : sort.getQueryColumnIndexes()) {
            if (idx < 0 || idx >= expressions.size()) {
                throw DbException.getInvalidValueException("ORDER BY", idx + 1);
            }
            Expression expr = expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) {
                return null;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            if (exprCol.getTableFilter() != topTableFilter) {
                return null;
            }
            sortColumns.add(exprCol.getColumn());
        }
        Column[] sortCols = sortColumns.toArray(new Column[0]);
        if (sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(session);
        }
        ArrayList<Index> list = topTableFilter.getTable().getIndexes();
        if (list != null) {
            int[] sortTypes = sort.getSortTypesWithNullPosition();
            for (Index index : list) {
                if (index.getCreateSQL() == null) {
                    // can't use the scan index
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    continue;
                }
                IndexColumn[] indexCols = index.getIndexColumns();
                if (indexCols.length < sortCols.length) {
                    continue;
                }
                boolean ok = true;
                for (int j = 0; j < sortCols.length; j++) {
                    // the index and the sort order must start
                    // with the exact same columns
                    IndexColumn idxCol = indexCols[j];
                    Column sortCol = sortCols[j];
                    if (idxCol.column != sortCol) {
                        ok = false;
                        break;
                    }
                    if (SortOrder.addExplicitNullPosition(idxCol.sortType) != sortTypes[j]) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    return index;
                }
            }
        }
        if (sortCols.length == 1 && sortCols[0].getColumnId() == -1) {
            // special case: order by _ROWID_
            Index index = topTableFilter.getTable().getScanIndex(session);
            if (index.isRowIdIndex()) {
                return index;
            }
        }
        return null;
    }

    private void queryDistinct(ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        SearchRow first = null;
        int columnIndex = index.getColumns()[0].getColumnId();
        int sampleSize = getSampleSizeValue(session);
        while (true) {
            setCurrentRowNumber(rowNumber + 1);
            Cursor cursor = index.findNext(session, first, null);
            if (!cursor.next()) {
                break;
            }
            SearchRow found = cursor.getSearchRow();
            Value value = found.getValue(columnIndex);
            if (first == null) {
                first = topTableFilter.getTable().getTemplateSimpleRow(true);
            }
            first.setValue(columnIndex, value);
            Value[] row = { value };
            result.addRow(row);
            rowNumber++;
            if ((sort == null || sortUsingIndex) && limitRows > 0 &&
                    rowNumber >= limitRows) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
    }

    private LazyResult queryFlat(int columnCount, ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        ArrayList<Row> forUpdateRows = null;
        if (isForUpdateMvcc) {
            forUpdateRows = New.arrayList();
        }
        int sampleSize = getSampleSizeValue(session);
        LazyResultQueryFlat lazyResult = new LazyResultQueryFlat(expressionArray,
                sampleSize, columnCount);
        if (result == null) {
            return lazyResult;
        }
        while (lazyResult.next()) {
            if (isForUpdateMvcc) {
                topTableFilter.lockRowAdd(forUpdateRows);
            }
            result.addRow(lazyResult.currentRow());
            if ((sort == null || sortUsingIndex) && limitRows > 0 &&
                    result.getRowCount() >= limitRows) {
                break;
            }
        }
        if (isForUpdateMvcc) {
            topTableFilter.lockRows(forUpdateRows);
        }
        return null;
    }

    private void queryQuick(int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressionArray,
                visibleColumnCount);
        result.done();
        return result;
    }

    @Override
    protected ResultInterface queryWithoutCache(int maxRows, ResultTarget target) {
        int limitRows = maxRows == 0 ? -1 : maxRows;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            int l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            if (limitRows < 0) {
                limitRows = l;
            } else if (l >= 0) {
                limitRows = Math.min(l, limitRows);
            }
        }
        boolean lazy = session.isLazyQueryExecution() &&
                target == null && !isForUpdate && !isQuickAggregateQuery &&
                limitRows != 0 && offsetExpr == null && isReadOnly();
        int columnCount = expressions.size();
        LocalResult result = null;
        if (!lazy && (target == null ||
                !session.getDatabase().getSettings().optimizeInsertFromSelect)) {
            result = createLocalResult(result);
        }
        if (sort != null && (!sortUsingIndex || distinct)) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
        }
        if (distinct && !isDistinctQuery) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (randomAccessResult) {
            result = createLocalResult(result);
        }
        if (isGroupQuery && !isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (!lazy && (limitRows >= 0 || offsetExpr != null)) {
            result = createLocalResult(result);
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        boolean exclusive = isForUpdate && !isForUpdateMvcc;
        if (isForUpdateMvcc) {
            if (isGroupQuery) {
                throw DbException.getUnsupportedException(
                        "MVCC=TRUE && FOR UPDATE && GROUP");
            } else if (distinct) {
                throw DbException.getUnsupportedException(
                        "MVCC=TRUE && FOR UPDATE && DISTINCT");
            } else if (isQuickAggregateQuery) {
                throw DbException.getUnsupportedException(
                        "MVCC=TRUE && FOR UPDATE && AGGREGATE");
            } else if (topTableFilter.getJoin() != null) {
                throw DbException.getUnsupportedException(
                        "MVCC=TRUE && FOR UPDATE && JOIN");
            }
        }
        topTableFilter.lock(session, exclusive, exclusive);
        ResultTarget to = result != null ? result : target;
        lazy &= to == null;
        LazyResult lazyResult = null;
        if (limitRows != 0) {
            try {
                if (isQuickAggregateQuery) {
                    queryQuick(columnCount, to);
                } else if (isGroupQuery) {
                    if (isGroupSortedQuery) {
                        lazyResult = queryGroupSorted(columnCount, to);
                    } else {
                        queryGroup(columnCount, result);
                    }
                } else if (isDistinctQuery) {
                    queryDistinct(to, limitRows);
                } else {
                    lazyResult = queryFlat(columnCount, to, limitRows);
                }
            } finally {
                if (!lazy) {
                    resetJoinBatchAfterQuery();
                }
            }
        }
        assert lazy == (lazyResult != null): lazy;
        if (lazyResult != null) {
            if (limitRows > 0) {
                lazyResult.setLimit(limitRows);
            }
            return lazyResult;
        }
        if (offsetExpr != null) {
            result.setOffset(offsetExpr.getValue(session).getInt());
        }
        if (limitRows >= 0) {
            result.setLimit(limitRows);
        }
        if (result != null) {
            result.done();
            if (target != null) {
                while (result.next()) {
                    target.addRow(result.currentRow());
                }
                result.close();
                return null;
            }
            return result;
        }
        return null;
    }

    /**
     * Reset the batch-join after the query result is closed.
     */
    void resetJoinBatchAfterQuery() {
        JoinBatch jb = getJoinBatch();
        if (jb != null) {
            jb.reset(false);
        }
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, expressionArray,
                visibleColumnCount);
    }

    private void expandColumnList() {
        Database db = session.getDatabase();

        // the expressions may change within the loop
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            String schemaName = expr.getSchemaName();
            String tableAlias = expr.getTableAlias();
            if (tableAlias == null) {
                expressions.remove(i);
                for (TableFilter filter : filters) {
                    i = expandColumnList(filter, i);
                }
                i--;
            } else {
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    if (db.equalsIdentifiers(tableAlias, f.getTableAlias())) {
                        if (schemaName == null ||
                                db.equalsIdentifiers(schemaName,
                                        f.getSchemaName())) {
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1,
                            tableAlias);
                }
                expressions.remove(i);
                i = expandColumnList(filter, i);
                i--;
            }
        }
    }

    private int expandColumnList(TableFilter filter, int index) {
        Table t = filter.getTable();
        String alias = filter.getTableAlias();
        Column[] columns = t.getColumns();
        for (Column c : columns) {
            if (!c.getVisible()) {
                continue;
            }
            if (filter.isNaturalJoinColumn(c)) {
                continue;
            }
            String name = filter.getDerivedColumnName(c);
            ExpressionColumn ec = new ExpressionColumn(
                    session.getDatabase(), null, alias, name != null ? name : c.getName());
            expressions.add(index++, ec);
        }
        return index;
    }

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ArrayList<String> expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = New.arrayList();
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
        } else {
            expressionSQL = null;
        }
        if (orderList != null) {
            initOrder(session, expressions, expressionSQL, orderList,
                    visibleColumnCount, distinct, filters);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }

        Database db = session.getDatabase();

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            int size = group.size();
            int expSize = expressionSQL.size();
            groupIndex = new int[size];
            for (int i = 0; i < size; i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = j;
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressions.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                        sql = expr.getAlias();
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                    }
                }
                if (found < 0) {
                    int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (TableFilter f : filters) {
            mapColumns(f, 0);
        }
        if (havingIndex >= 0) {
            Expression expr = expressions.get(havingIndex);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0);
        }
        checkInit = true;
    }

    @Override
    public void prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        ColumnNamer columnNamer = new ColumnNamer(session);
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            String proposedColumnName = e.getAlias();
            String columnName = columnNamer.getColumnName(e, i, proposedColumnName);
            // if the name changed, create an alias
            if (!columnName.equals(proposedColumnName)) {
                e = new Alias(e, columnName, true);
            }
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            for (TableFilter f : filters) {
                // outer joins: must not add index conditions such as
                // "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    condition.createIndexConditions(session, f);
                }
            }
        }
        if (isGroupQuery && groupIndex == null &&
                havingIndex < 0 && filters.size() == 1) {
            if (condition == null) {
                Table t = filters.get(0).getTable();
                ExpressionVisitor optimizable = ExpressionVisitor.
                        getOptimizableVisitor(t);
                isQuickAggregateQuery = isEverything(optimizable);
            }
        }
        cost = preparePlan(session.isParsingCreateView());
        if (distinct && session.getDatabase().getSettings().optimizeDistinct &&
                !isGroupQuery && filters.size() == 1 &&
                expressions.size() == 1 && condition == null) {
            Expression expr = expressions.get(0);
            expr = expr.getNonAliasExpression();
            if (expr instanceof ExpressionColumn) {
                Column column = ((ExpressionColumn) expr).getColumn();
                int selectivity = column.getSelectivity();
                Index columnIndex = topTableFilter.getTable().
                        getIndexForColumn(column, false, true);
                if (columnIndex != null &&
                        selectivity != Constants.SELECTIVITY_DEFAULT &&
                        selectivity < 20) {
                    // the first column must be ascending
                    boolean ascending = columnIndex.
                            getIndexColumns()[0].sortType == SortOrder.ASCENDING;
                    Index current = topTableFilter.getIndex();
                    // if another index is faster
                    if (columnIndex.canFindNext() && ascending &&
                            (current == null ||
                            current.getIndexType().isScan() ||
                            columnIndex == current)) {
                        IndexType type = columnIndex.getIndexType();
                        // hash indexes don't work, and unique single column
                        // indexes don't work
                        if (!type.isHash() && (!type.isUnique() ||
                                columnIndex.getColumns().length > 1)) {
                            topTableFilter.setIndex(columnIndex);
                            isDistinctQuery = true;
                        }
                    }
                }
            }
        }
        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            Index index = getSortIndex();
            Index current = topTableFilter.getIndex();
            if (index != null && current != null) {
                if (current.getIndexType().isScan() || current == index) {
                    topTableFilter.setIndex(index);
                    if (!topTableFilter.hasInComparisons()) {
                        // in(select ...) and in(1,2,3) may return the key in
                        // another order
                        sortUsingIndex = true;
                    }
                } else if (index.getIndexColumns() != null
                        && index.getIndexColumns().length >= current
                                .getIndexColumns().length) {
                    IndexColumn[] sortColumns = index.getIndexColumns();
                    IndexColumn[] currentColumns = current.getIndexColumns();
                    boolean swapIndex = false;
                    for (int i = 0; i < currentColumns.length; i++) {
                        if (sortColumns[i].column != currentColumns[i].column) {
                            swapIndex = false;
                            break;
                        }
                        if (sortColumns[i].sortType != currentColumns[i].sortType) {
                            swapIndex = true;
                        }
                    }
                    if (swapIndex) {
                        topTableFilter.setIndex(index);
                        sortUsingIndex = true;
                    }
                }
            }
        }
        if (!isQuickAggregateQuery && isGroupQuery &&
                getGroupByExpressionCount() > 0) {
            Index index = getGroupSortedIndex();
            Index current = topTableFilter.getIndex();
            if (index != null && current != null && (current.getIndexType().isScan() ||
                    current == index)) {
                topTableFilter.setIndex(index);
                isGroupSortedQuery = true;
            }
        }
        expressionArray = expressions.toArray(new Expression[0]);
        isPrepared = true;
    }

    @Override
    public void prepareJoinBatch() {
        ArrayList<TableFilter> list = New.arrayList();
        TableFilter f = getTopTableFilter();
        do {
            if (f.getNestedJoin() != null) {
                // we do not support batching with nested joins
                return;
            }
            list.add(f);
            f = f.getJoin();
        } while (f != null);
        TableFilter[] fs = list.toArray(new TableFilter[0]);
        // prepare join batch
        JoinBatch jb = null;
        for (int i = fs.length - 1; i >= 0; i--) {
            jb = fs[i].prepareJoinBatch(jb, fs, i);
        }
    }

    public JoinBatch getJoinBatch() {
        return getTopTableFilter().getJoinBatch();
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = new HashSet<>();
        for (TableFilter filter : filters) {
            set.add(filter.getTable());
        }
        return set;
    }

    @Override
    public void fireBeforeSelectTriggers() {
        for (TableFilter filter : filters) {
            filter.getTable().fire(session, Trigger.SELECT, true);
        }
    }

    private double preparePlan(boolean parse) {
        TableFilter[] topArray = topFilters.toArray(new TableFilter[0]);
        for (TableFilter t : topArray) {
            t.setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize(parse);
        topTableFilter = optimizer.getTopFilter();
        double planCost = optimizer.getCost();

        setEvaluatableRecursive(topTableFilter);

        if (!parse) {
            topTableFilter.prepare();
        }
        return planCost;
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    // need to check that all added are bound to a table
                    on = on.optimize(session);
                    if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                        f.removeJoinCondition();
                        addCondition(on);
                    }
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressions) {
                e.setEvaluatable(f, true);
            }
        }
    }

    @Override
    public String getPlanSQL() {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(new Expression[0]);
        StatementBuilder buff = new StatementBuilder();
        for (TableFilter f : topFilters) {
            Table t = f.getTable();
            TableView tableView = t.isView() ? (TableView) t : null;
            if (tableView != null && tableView.isRecursive() && tableView.isTableExpression()) {

                if (tableView.isPersistent()) {
                    // skip the generation of plan SQL for this already recursive persistent CTEs,
                    // since using a with statement will re-create the common table expression
                    // views.
                } else {
                    buff.append("WITH RECURSIVE ").append(t.getName()).append('(');
                    buff.resetCount();
                    for (Column c : t.getColumns()) {
                        buff.appendExceptFirst(",");
                        buff.append(c.getName());
                    }
                    buff.append(") AS ").append(t.getSQL()).append("\n");
                }
            }
        }
        buff.resetCount();
        buff.append("SELECT");
        if (distinct) {
            buff.append(" DISTINCT");
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(exprList[i].getSQL(), 4, false));
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            buff.resetCount();
            int i = 0;
            do {
                buff.appendExceptFirst("\n");
                buff.append(filter.getPlanSQL(i++ > 0));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : topFilters) {
                do {
                    buff.appendExceptFirst("\n");
                    buff.append(f.getPlanSQL(i++ > 0));
                    f = f.getJoin();
                } while (f != null);
            }
        }
        if (condition != null) {
            buff.append("\nWHERE ").append(
                    StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING ").append(
                    StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING ").append(
                    StringUtils.unEnclose(h.getSQL()));
        }
        if (sort != null) {
            buff.append("\nORDER BY ").append(
                    sort.getSQL(exprList, visibleColumnCount));
        }
        if (orderList != null) {
            buff.append("\nORDER BY ");
            buff.resetCount();
            for (SelectOrderBy o : orderList) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(o.getSQL()));
            }
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(
                    StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append(" OFFSET ").append(
                        StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(
                    StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        if (isQuickAggregateQuery) {
            buff.append("\n/* direct lookup */");
        }
        if (isDistinctQuery) {
            buff.append("\n/* distinct */");
        }
        if (sortUsingIndex) {
            buff.append("\n/* index sorted */");
        }
        if (isGroupQuery) {
            if (isGroupSortedQuery) {
                buff.append("\n/* group sorted */");
            }
        }
        // buff.append("\n/* cost: " + cost + " */");
        return buff.toString();
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public Expression getHaving() {
        return having;
    }

    @Override
    public int getColumnCount() {
        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
        if (session.getDatabase().getSettings().selectForUpdateMvcc &&
                session.getDatabase().isMultiVersion()) {
            isForUpdateMvcc = b;
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressions) {
            e.setEvaluatable(tableFilter, b);
        }
        if (condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    /**
     * Check if this is an aggregate query with direct lookup, for example a
     * query of the type SELECT COUNT(*) FROM TEST or
     * SELECT MAX(ID) FROM TEST.
     *
     * @return true if a direct lookup is possible
     */
    public boolean isQuickAggregateQuery() {
        return isQuickAggregateQuery;
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId,
            int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        if (col.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
            comp = new Comparison(session, comparisonType, col, param);
        } else {
            // this condition will always evaluate to true, but need to
            // add the parameter, so it can be set later
            comp = new Comparison(session, Comparison.EQUAL_NULL_SAFE, param, param);
        }
        comp = comp.optimize(session);
        boolean addToCondition = true;
        if (isGroupQuery) {
            addToCondition = false;
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    addToCondition = true;
                    break;
                }
            }
            if (!addToCondition) {
                if (havingIndex >= 0) {
                    having = expressions.get(havingIndex);
                }
                if (having == null) {
                    having = comp;
                } else {
                    having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
                }
            }
        }
        if (addToCondition) {
            if (condition == null) {
                condition = comp;
            } else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    @Override
    public void updateAggregate(Session s) {
        for (Expression e : expressions) {
            e.updateAggregate(s);
        }
        if (condition != null) {
            condition.updateAggregate(s);
        }
        if (having != null) {
            having.updateAggregate(s);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC: {
            if (isForUpdate) {
                return false;
            }
            for (TableFilter f : filters) {
                if (!f.getTable().isDeterministic()) {
                    return false;
                }
            }
            break;
        }
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID: {
            for (TableFilter f : filters) {
                long m = f.getTable().getMaxDataModificationId();
                visitor.addDataModificationId(m);
            }
            break;
        }
        case ExpressionVisitor.EVALUATABLE: {
            if (!session.getDatabase().getSettings().optimizeEvaluatableSubqueries) {
                return false;
            }
            break;
        }
        case ExpressionVisitor.GET_DEPENDENCIES: {
            for (TableFilter f : filters) {
                Table table = f.getTable();
                visitor.addDependency(table);
                table.addDependencies(visitor.getDependencies());
            }
            break;
        }
        default:
        }
        ExpressionVisitor v2 = visitor.incrementQueryLevel(1);
        boolean result = true;
        for (Expression e : expressions) {
            if (!e.isEverything(v2)) {
                result = false;
                break;
            }
        }
        if (result && condition != null && !condition.isEverything(v2)) {
            result = false;
        }
        if (result && having != null && !having.isEverything(v2)) {
            result = false;
        }
        return result;
    }

    @Override
    public boolean isReadOnly() {
        return isEverything(ExpressionVisitor.READONLY_VISITOR);
    }


    @Override
    public boolean isCacheable() {
        return !isForUpdate;
    }

    @Override
    public int getType() {
        return CommandInterface.SELECT;
    }

    @Override
    public boolean allowGlobalConditions() {
        return offsetExpr == null && (limitExpr == null || sort == null);
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    /**
     * Lazy execution for this select.
     */
    private abstract class LazyResultSelect extends LazyResult {

        int rowNumber;
        int columnCount;

        LazyResultSelect(Expression[] expressions, int columnCount) {
            super(expressions);
            this.columnCount = columnCount;
            setCurrentRowNumber(0);
        }

        @Override
        public final int getVisibleColumnCount() {
            return visibleColumnCount;
        }

        @Override
        public void close() {
            if (!isClosed()) {
                super.close();
                resetJoinBatchAfterQuery();
            }
        }

        @Override
        public void reset() {
            super.reset();
            resetJoinBatchAfterQuery();
            topTableFilter.reset();
            setCurrentRowNumber(0);
            rowNumber = 0;
        }
    }

    /**
     * Lazy execution for a flat query.
     */
    private final class LazyResultQueryFlat extends LazyResultSelect {

        int sampleSize;

        LazyResultQueryFlat(Expression[] expressions, int sampleSize, int columnCount) {
            super(expressions, columnCount);
            this.sampleSize = sampleSize;
        }

        @Override
        protected Value[] fetchNextRow() {
            while ((sampleSize <= 0 || rowNumber < sampleSize) &&
                    topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                if (isConditionMet()) {
                    ++rowNumber;
                    Value[] row = new Value[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        Expression expr = expressions.get(i);
                        row[i] = expr.getValue(getSession());
                    }
                    return row;
                }
            }
            return null;
        }
    }

    /**
     * Lazy execution for a group sorted query.
     */
    private final class LazyResultGroupSorted extends LazyResultSelect {

        Value[] previousKeyValues;

        LazyResultGroupSorted(Expression[] expressions, int columnCount) {
            super(expressions, columnCount);
            currentGroup = null;
        }

        @Override
        public void reset() {
            super.reset();
            currentGroup = null;
        }

        @Override
        protected Value[] fetchNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                if (isConditionMet()) {
                    rowNumber++;
                    Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(getSession());
                    }

                    Value[] row = null;
                    if (previousKeyValues == null) {
                        previousKeyValues = keyValues;
                        currentGroup = new HashMap<>();
                    } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                        row = createGroupSortedRow(previousKeyValues, columnCount);
                        previousKeyValues = keyValues;
                        currentGroup = new HashMap<>();
                    }
                    currentGroupRowId++;

                    for (int i = 0; i < columnCount; i++) {
                        if (groupByExpression == null || !groupByExpression[i]) {
                            Expression expr = expressions.get(i);
                            expr.updateAggregate(getSession());
                        }
                    }
                    if (row != null) {
                        return row;
                    }
                }
            }
            Value[] row = null;
            if (previousKeyValues != null) {
                row = createGroupSortedRow(previousKeyValues, columnCount);
                previousKeyValues = null;
            }
            return row;
        }
    }
}
