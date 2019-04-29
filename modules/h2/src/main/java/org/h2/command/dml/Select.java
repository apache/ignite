/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.Wildcard;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.Window;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.ViewIndex;
import org.h2.message.DbException;
import org.h2.result.LazyResult;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.IndexColumn;
import org.h2.table.JoinBatch;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableFilter.TableFilterVisitor;
import org.h2.table.TableType;
import org.h2.table.TableView;
import org.h2.util.ColumnNamer;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

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

    private final ArrayList<TableFilter> filters = Utils.newSmallArrayList();
    private final ArrayList<TableFilter> topFilters = Utils.newSmallArrayList();

    /**
     * Parent select for selects in table filters.
     */
    private Select parentSelect;

    /**
     * WHERE condition.
     */
    private Expression condition;

    /**
     * HAVING condition.
     */
    private Expression having;

    /**
     * QUALIFY condition.
     */
    private Expression qualify;

    /**
     * The visible columns (the ones required in the result).
     */
    int visibleColumnCount;

    /**
     * {@code DISTINCT ON(...)} expressions.
     */
    private Expression[] distinctExpressions;

    private int[] distinctIndexes;

    private int distinctColumnCount;
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
     * Grouped data for aggregates.
     */
    SelectGroups groupData;

    private int havingIndex;

    private int qualifyIndex;

    private int[] groupByCopies;

    /**
     * This flag is set when SELECT statement contains (non-window) aggregate
     * functions, GROUP BY clause or HAVING clause.
     */
    boolean isGroupQuery;
    private boolean isGroupSortedQuery;
    private boolean isWindowQuery;
    private boolean isForUpdate, isForUpdateMvcc;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery;
    private boolean isPrepared, checkInit;
    private boolean sortUsingIndex;

    private boolean isGroupWindowStage2;

    private HashMap<String, Window> windows;

    public Select(Session session, Select parentSelect) {
        super(session);
        this.parentSelect = parentSelect;
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
     * Sets a wildcard expression as in "SELECT * FROM TEST".
     */
    public void setWildcard() {
        expressions = new ArrayList<>(1);
        expressions.add(new Wildcard(null, null));
    }

    /**
     * Set when SELECT statement contains (non-window) aggregate functions,
     * GROUP BY clause or HAVING clause.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    /**
     * Called if this query contains window functions.
     */
    public void setWindowQuery() {
        isWindowQuery = true;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public ArrayList<Expression> getGroupBy() {
        return group;
    }

    /**
     * Get the group data if there is currently a group-by active.
     *
     * @param window is this a window function
     * @return the grouped data
     */
    public SelectGroups getGroupDataIfCurrent(boolean window) {
        return groupData != null && (window || groupData.isCurrentGroup()) ? groupData : null;
    }

    @Override
    public void setDistinct() {
        if (distinctExpressions != null) {
            throw DbException.getUnsupportedException("DISTINCT ON together with DISTINCT");
        }
        distinct = true;
    }

    /**
     * Set the DISTINCT ON expressions.
     *
     * @param distinctExpressions array of expressions
     */
    public void setDistinct(Expression[] distinctExpressions) {
        if (distinct) {
            throw DbException.getUnsupportedException("DISTINCT ON together with DISTINCT");
        }
        this.distinctExpressions = distinctExpressions;
    }

    @Override
    public void setDistinctIfPossible() {
        if (!isAnyDistinct() && offsetExpr == null && limitExpr == null) {
            distinct = true;
        }
    }

    @Override
    public boolean isAnyDistinct() {
        return distinct || distinctExpressions != null;
    }

    /**
     * Adds a named window definition.
     *
     * @param name name
     * @param window window definition
     * @return true if a new definition was added, false if old definition was replaced
     */
    public boolean addWindow(String name, Window window) {
        if (windows == null) {
            windows = new HashMap<>();
        }
        return windows.put(name, window) == null;
    }

    /**
     * Returns a window with specified name, or null.
     *
     * @param name name of the window
     * @return the window with specified name, or null
     */
    public Window getWindow(String name) {
        return windows != null ? windows.get(name) : null;
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

    private LazyResult queryGroupSorted(int columnCount, ResultTarget result, long offset, boolean quickOffset) {
        LazyResultGroupSorted lazyResult = new LazyResultGroupSorted(expressionArray, columnCount);
        skipOffset(lazyResult, offset, quickOffset);
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

    boolean isConditionMetForUpdate() {
        if (isConditionMet()) {
            int count = filters.size();
            boolean notChanged = true;
            for (int i = 0; i < count; i++) {
                TableFilter tableFilter = filters.get(i);
                if (!tableFilter.isJoinOuter() && !tableFilter.isJoinOuterIndirect()) {
                    Row row = tableFilter.get();
                    Table table = tableFilter.getTable();
                    // Views, function tables, links, etc. do not support locks
                    if (table.isMVStore()) {
                        Row lockedRow = table.lockRow(session, row);
                        if (lockedRow == null) {
                            return false;
                        }
                        if (!row.hasSharedData(lockedRow)) {
                            tableFilter.set(lockedRow);
                            notChanged = false;
                        }
                    }
                }
            }
            return notChanged || isConditionMet();
        }
        return false;
    }

    boolean isConditionMet() {
        return condition == null || condition.getBooleanValue(session);
    }

    private void queryWindow(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_WINDOW);
            processGroupResult(columnCount, result, offset, quickOffset, false);
        } finally {
            groupData.reset();
        }
    }

    private void queryGroupWindow(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_GROUP);
            try {
                isGroupWindowStage2 = true;
                while (groupData.next() != null) {
                    if (havingIndex < 0 || expressions.get(havingIndex).getBooleanValue(session)) {
                        updateAgg(columnCount, DataAnalysisOperation.STAGE_WINDOW);
                    } else {
                        groupData.remove();
                    }
                }
                groupData.done();
                processGroupResult(columnCount, result, offset, quickOffset, /* Having was performed earlier */ false);
            } finally {
                isGroupWindowStage2 = false;
            }
        } finally {
            groupData.reset();
        }
    }

    private void queryGroup(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_GROUP);
            processGroupResult(columnCount, result, offset, quickOffset, true);
        } finally {
            groupData.reset();
        }
    }

    private void initGroupData(int columnCount) {
        if (groupData == null) {
            setGroupData(SelectGroups.getInstance(session, expressions, isGroupQuery, groupIndex));
        } else {
            updateAgg(columnCount, DataAnalysisOperation.STAGE_RESET);
        }
        groupData.reset();
    }

    void setGroupData(final SelectGroups groupData) {
        this.groupData = groupData;
        topTableFilter.visit(new TableFilterVisitor() {
            @Override
            public void accept(TableFilter f) {
                Select s = f.getSelect();
                if (s != null) {
                    s.groupData = groupData;
                }
            }
        });
    }

    private void gatherGroup(int columnCount, int stage) {
        long rowNumber = 0;
        setCurrentRowNumber(0);
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (isForUpdateMvcc ? isConditionMetForUpdate() : isConditionMet()) {
                rowNumber++;
                groupData.nextSource();
                updateAgg(columnCount, stage);
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        groupData.done();
    }


    /**
     * Update any aggregate expressions with the query stage.
     * @param columnCount number of columns
     * @param stage see STAGE_RESET/STAGE_GROUP/STAGE_WINDOW in DataAnalysisOperation
     */
    void updateAgg(int columnCount, int stage) {
        for (int i = 0; i < columnCount; i++) {
            if ((groupByExpression == null || !groupByExpression[i])
                    && (groupByCopies == null || groupByCopies[i] < 0)) {
                Expression expr = expressions.get(i);
                expr.updateAggregate(session, stage);
            }
        }
    }

    private void processGroupResult(int columnCount, LocalResult result, long offset, boolean quickOffset,
            boolean withHaving) {
        for (ValueRow currentGroupsKey; (currentGroupsKey = groupData.next()) != null;) {
            Value[] keyValues = currentGroupsKey.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                if (groupByCopies != null) {
                    int original = groupByCopies[j];
                    if (original >= 0) {
                        row[j] = row[original];
                        continue;
                    }
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (withHaving && isHavingNullOrFalse(row)) {
                continue;
            }
            if (qualifyIndex >= 0 && !row[qualifyIndex].getBoolean()) {
                continue;
            }
            if (quickOffset && offset > 0) {
                offset--;
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
        ArrayList<Column> sortColumns = Utils.newSmallArrayList();
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

    private void queryDistinct(ResultTarget result, long offset, long limitRows, boolean withTies,
            boolean quickOffset) {
        if (limitRows > 0 && offset > 0) {
            limitRows += offset;
            if (limitRows < 0) {
                // Overflow
                limitRows = Long.MAX_VALUE;
            }
        }
        long rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        SearchRow first = null;
        int columnIndex = index.getColumns()[0].getColumnId();
        int sampleSize = getSampleSizeValue(session);
        if (!quickOffset) {
            offset = 0;
        }
        while (true) {
            setCurrentRowNumber(++rowNumber);
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
            if (offset > 0) {
                offset--;
                continue;
            }
            Value[] row = { value };
            result.addRow(row);
            if ((sort == null || sortUsingIndex) && limitRows > 0 &&
                    rowNumber >= limitRows && !withTies) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
    }

    private LazyResult queryFlat(int columnCount, ResultTarget result, long offset, long limitRows, boolean withTies,
            boolean quickOffset) {
        if (limitRows > 0 && offset > 0 && !quickOffset) {
            limitRows += offset;
            if (limitRows < 0) {
                // Overflow
                limitRows = Long.MAX_VALUE;
            }
        }
        int sampleSize = getSampleSizeValue(session);
        LazyResultQueryFlat lazyResult = new LazyResultQueryFlat(expressionArray, columnCount, sampleSize,
                isForUpdateMvcc);
        skipOffset(lazyResult, offset, quickOffset);
        if (result == null) {
            return lazyResult;
        }
        if (limitRows < 0 || sort != null && !sortUsingIndex || withTies && !quickOffset) {
            limitRows = Long.MAX_VALUE;
        }
        Value[] row = null;
        while (result.getRowCount() < limitRows && lazyResult.next()) {
            row = lazyResult.currentRow();
            result.addRow(row);
        }
        if (limitRows != Long.MAX_VALUE && withTies && sort != null && row != null) {
            Value[] expected = row;
            while (lazyResult.next()) {
                row = lazyResult.currentRow();
                if (sort.compare(expected, row) != 0) {
                    break;
                }
                result.addRow(row);
            }
            result.limitsWereApplied();
        }
        return null;
    }

    private static void skipOffset(LazyResultSelect lazyResult, long offset, boolean quickOffset) {
        if (quickOffset) {
            while (offset > 0 && lazyResult.skip()) {
                offset--;
            }
        }
    }

    private void queryQuick(int columnCount, ResultTarget result, boolean skipResult) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        if (!skipResult) {
            result.addRow(row);
        }
    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result = session.getDatabase().getResultFactory().create(session, expressionArray,
                visibleColumnCount);
        result.done();
        return result;
    }

    @Override
    protected ResultInterface queryWithoutCache(int maxRows, ResultTarget target) {
        disableLazyForJoinSubqueries(topTableFilter);

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
        boolean fetchPercent = this.fetchPercent;
        if (fetchPercent) {
            // Need to check it now, because negative limit has special treatment later
            if (limitRows < 0 || limitRows > 100) {
                throw DbException.getInvalidValueException("FETCH PERCENT", limitRows);
            }
            // 0 PERCENT means 0
            if (limitRows == 0) {
                fetchPercent = false;
            }
        }
        long offset;
        if (offsetExpr != null) {
            offset = offsetExpr.getValue(session).getLong();
            if (offset < 0) {
                offset = 0;
            }
        } else {
            offset = 0;
        }
        boolean lazy = session.isLazyQueryExecution() &&
                target == null && !isForUpdate && !isQuickAggregateQuery &&
                limitRows != 0 && !fetchPercent && !withTies && offset == 0 && isReadOnly();
        int columnCount = expressions.size();
        LocalResult result = null;
        if (!lazy && (target == null ||
                !session.getDatabase().getSettings().optimizeInsertFromSelect)) {
            result = createLocalResult(result);
        }
        // Do not add rows before OFFSET to result if possible
        boolean quickOffset = !fetchPercent;
        if (sort != null && (!sortUsingIndex || isAnyDistinct())) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
            if (!sortUsingIndex) {
                quickOffset = false;
            }
        }
        if (distinct) {
            if (!isDistinctQuery) {
                quickOffset = false;
                result = createLocalResult(result);
                result.setDistinct();
            }
        } else if (distinctExpressions != null) {
            quickOffset = false;
            result = createLocalResult(result);
            result.setDistinct(distinctIndexes);
        }
        if (isWindowQuery || isGroupQuery && !isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (!lazy && (limitRows >= 0 || offset > 0)) {
            result = createLocalResult(result);
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        boolean exclusive = isForUpdate && !isForUpdateMvcc;
        topTableFilter.lock(session, exclusive, exclusive);
        ResultTarget to = result != null ? result : target;
        lazy &= to == null;
        LazyResult lazyResult = null;
        if (limitRows != 0) {
            // Cannot apply limit now if percent is specified
            int limit = fetchPercent ? -1 : limitRows;
            try {
                if (isQuickAggregateQuery) {
                    queryQuick(columnCount, to, quickOffset && offset > 0);
                } else if (isWindowQuery) {
                    if (isGroupQuery) {
                        queryGroupWindow(columnCount, result, offset, quickOffset);
                    } else {
                        queryWindow(columnCount, result, offset, quickOffset);
                    }
                } else if (isGroupQuery) {
                    if (isGroupSortedQuery) {
                        lazyResult = queryGroupSorted(columnCount, to, offset, quickOffset);
                    } else {
                        queryGroup(columnCount, result, offset, quickOffset);
                    }
                } else if (isDistinctQuery) {
                    queryDistinct(to, offset, limit, withTies, quickOffset);
                } else {
                    lazyResult = queryFlat(columnCount, to, offset, limit, withTies, quickOffset);
                }
                if (quickOffset) {
                    offset = 0;
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
            if (randomAccessResult) {
                return convertToDistinct(lazyResult);
            } else {
                return lazyResult;
            }
        }
        if (offset != 0) {
            if (offset > Integer.MAX_VALUE) {
                throw DbException.getInvalidValueException("OFFSET", offset);
            }
            result.setOffset((int) offset);
        }
        if (limitRows >= 0) {
            result.setLimit(limitRows);
            result.setFetchPercent(fetchPercent);
            if (withTies) {
                result.setWithTies(sort);
            }
        }
        if (result != null) {
            result.done();
            if (randomAccessResult && !distinct) {
                result = convertToDistinct(result);
            }
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

    private void disableLazyForJoinSubqueries(final TableFilter top) {
        if (session.isLazyQueryExecution()) {
            top.visit(new TableFilter.TableFilterVisitor() {
                @Override
                public void accept(TableFilter f) {
                    if (f != top && f.getTable().getTableType() == TableType.VIEW) {
                        ViewIndex idx = (ViewIndex) f.getIndex();
                        if (idx != null && idx.getQuery() != null) {
                            idx.getQuery().setNeverLazy(true);
                        }
                    }
                }
            });
        }
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
        return old != null ? old : session.getDatabase().getResultFactory().create(session, expressionArray,
                visibleColumnCount);
    }

    private LocalResult convertToDistinct(ResultInterface result) {
        LocalResult distinctResult = session.getDatabase().getResultFactory().create(session,
            expressionArray, visibleColumnCount);
        distinctResult.setDistinct();
        result.reset();
        while (result.next()) {
            distinctResult.addRow(result.currentRow());
        }
        result.close();
        distinctResult.done();
        return distinctResult;
    }

    private void expandColumnList() {
        // the expressions may change within the loop
        for (int i = 0; i < expressions.size();) {
            Expression expr = expressions.get(i);
            if (!(expr instanceof Wildcard)) {
                i++;
                continue;
            }
            expressions.remove(i);
            Wildcard w = (Wildcard) expr;
            String tableAlias = w.getTableAlias();
            boolean hasExceptColumns = w.getExceptColumns() != null;
            HashMap<Column, ExpressionColumn> exceptTableColumns = null;
            if (tableAlias == null) {
                if (hasExceptColumns) {
                    for (TableFilter filter : filters) {
                        w.mapColumns(filter, 1, Expression.MAP_INITIAL);
                    }
                    exceptTableColumns = w.mapExceptColumns();
                }
                for (TableFilter filter : filters) {
                    i = expandColumnList(filter, i, exceptTableColumns);
                }
            } else {
                Database db = session.getDatabase();
                String schemaName = w.getSchemaName();
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    if (db.equalsIdentifiers(tableAlias, f.getTableAlias())) {
                        if (schemaName == null || db.equalsIdentifiers(schemaName, f.getSchemaName())) {
                            if (hasExceptColumns) {
                                w.mapColumns(f, 1, Expression.MAP_INITIAL);
                                exceptTableColumns = w.mapExceptColumns();
                            }
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                }
                i = expandColumnList(filter, i, exceptTableColumns);
            }
        }
    }

    private int expandColumnList(TableFilter filter, int index, HashMap<Column, ExpressionColumn> except) {
        String alias = filter.getTableAlias();
        for (Column c : filter.getTable().getColumns()) {
            if (except != null && except.remove(c) != null) {
                continue;
            }
            if (!c.getVisible()) {
                continue;
            }
            if (filter.isNaturalJoinColumn(c)) {
                continue;
            }
            String name = filter.getDerivedColumnName(c);
            ExpressionColumn ec = new ExpressionColumn(
                    session.getDatabase(), null, alias, name != null ? name : c.getName(), false);
            expressions.add(index++, ec);
        }
        return index;
    }

    @Override
    public void init() {
        if (checkInit) {
            DbException.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ArrayList<String> expressionSQL;
        if (distinctExpressions != null || orderList != null || group != null) {
            expressionSQL = new ArrayList<>(visibleColumnCount);
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL(true);
                expressionSQL.add(sql);
            }
        } else {
            expressionSQL = null;
        }
        if (distinctExpressions != null) {
            BitSet set = new BitSet();
            for (Expression e : distinctExpressions) {
                set.set(initExpression(session, expressions, expressionSQL, e, visibleColumnCount, false,
                        filters));
            }
            int idx = 0, cnt = set.cardinality();
            distinctIndexes = new int[cnt];
            for (int i = 0; i < cnt; i++) {
                idx = set.nextSetBit(idx);
                distinctIndexes[i] = idx;
                idx++;
            }
        }
        if (orderList != null) {
            initOrder(session, expressions, expressionSQL, orderList,
                    visibleColumnCount, isAnyDistinct(), filters);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }
        if (qualify != null) {
            expressions.add(qualify);
            qualifyIndex = expressions.size() - 1;
            qualify = null;
        } else {
            qualifyIndex = -1;
        }

        if (withTies && !hasOrder()) {
            throw DbException.get(ErrorCode.WITH_TIES_WITHOUT_ORDER_BY);
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
                String sql = expr.getSQL(true);
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = mergeGroupByExpressions(db, j, expressionSQL, false);
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressions.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = mergeGroupByExpressions(db, j, expressionSQL, true);
                            break;
                        }
                        sql = expr.getAlias();
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = mergeGroupByExpressions(db, j, expressionSQL, true);
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
            checkUsed: if (groupByCopies != null) {
                for (int i : groupByCopies) {
                    if (i >= 0) {
                        break checkUsed;
                    }
                }
                groupByCopies = null;
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
        mapCondition(havingIndex);
        mapCondition(qualifyIndex);
        checkInit = true;
    }

    private void mapCondition(int index) {
        if (index >= 0) {
            Expression expr = expressions.get(index);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0, Expression.MAP_INITIAL);
        }
    }

    private int mergeGroupByExpressions(Database db, int index, ArrayList<String> expressionSQL, boolean scanPrevious)
    {
        /*
         * -1: uniqueness of expression is not known yet
         *
         * -2: expression that is used as a source for a copy or does not have
         * copies
         *
         * >=0: expression is a copy of expression at this index
         */
        if (groupByCopies != null) {
            int c = groupByCopies[index];
            if (c >= 0) {
                return c;
            } else if (c == -2) {
                return index;
            }
        } else {
            groupByCopies = new int[expressionSQL.size()];
            Arrays.fill(groupByCopies, -1);
        }
        String sql = expressionSQL.get(index);
        if (scanPrevious) {
            /*
             * If expression was matched using an alias previous expressions may
             * be identical.
             */
            for (int i = 0; i < index; i++) {
                if (db.equalsIdentifiers(sql, expressionSQL.get(i))) {
                    index = i;
                    break;
                }
            }
        }
        int l = expressionSQL.size();
        for (int i = index + 1; i < l; i++) {
            if (db.equalsIdentifiers(sql, expressionSQL.get(i))) {
                groupByCopies[i] = index;
            }
        }
        groupByCopies[index] = -2;
        return index;
    }

    @Override
    public void prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (!checkInit) {
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
        if (isGroupQuery && groupIndex == null && havingIndex < 0 && qualifyIndex < 0 && condition == null
                && filters.size() == 1) {
            isQuickAggregateQuery = isEverything(ExpressionVisitor.getOptimizableVisitor(filters.get(0).getTable()));
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
            if (sortUsingIndex && isForUpdateMvcc && !topTableFilter.getIndex().isRowIdIndex()) {
                sortUsingIndex = false;
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
        ArrayList<TableFilter> list = new ArrayList<>();
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
            t.createIndexConditions();
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
    public String getPlanSQL(boolean alwaysQuote) {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(new Expression[0]);
        StringBuilder builder = new StringBuilder();
        for (TableFilter f : topFilters) {
            Table t = f.getTable();
            TableView tableView = t.isView() ? (TableView) t : null;
            if (tableView != null && tableView.isRecursive() && tableView.isTableExpression()) {

                if (!tableView.isTemporary()) {
                    // skip the generation of plan SQL for this already recursive persistent CTEs,
                    // since using a with statement will re-create the common table expression
                    // views.
                } else {
                    builder.append("WITH RECURSIVE ");
                    t.getSchema().getSQL(builder, alwaysQuote).append('.');
                    Parser.quoteIdentifier(builder, t.getName(), alwaysQuote).append('(');
                    Column.writeColumns(builder, t.getColumns(), alwaysQuote);
                    builder.append(") AS ");
                    t.getSQL(builder, alwaysQuote).append('\n');
                }
            }
        }
        builder.append("SELECT");
        if (isAnyDistinct()) {
            builder.append(" DISTINCT");
            if (distinctExpressions != null) {
                builder.append(" ON(");
                Expression.writeExpressions(builder, distinctExpressions, alwaysQuote);
                builder.append(')');
            }
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append('\n');
            StringUtils.indent(builder, exprList[i].getSQL( alwaysQuote), 4, false);
        }
        builder.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            int i = 0;
            do {
                if (i > 0) {
                    builder.append('\n');
                }
                filter.getPlanSQL(builder, i++ > 0, alwaysQuote);
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            int i = 0;
            for (TableFilter f : topFilters) {
                do {
                    if (i > 0) {
                        builder.append('\n');
                    }
                    f.getPlanSQL(builder, i++ > 0, alwaysQuote);
                    f = f.getJoin();
                } while (f != null);
            }
        }
        if (condition != null) {
            builder.append("\nWHERE ");
            condition.getUnenclosedSQL(builder, alwaysQuote);
        }
        if (groupIndex != null) {
            builder.append("\nGROUP BY ");
            for (int i = 0, l = groupIndex.length; i < l; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                exprList[groupIndex[i]].getNonAliasExpression().getUnenclosedSQL(builder, alwaysQuote);
            }
        } else if (group != null) {
            builder.append("\nGROUP BY ");
            for (int i = 0, l = group.size(); i < l; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                group.get(i).getUnenclosedSQL(builder, alwaysQuote);
            }
        }
        getFilterSQL(builder, "\nHAVING ", exprList, having, havingIndex);
        getFilterSQL(builder, "\nQUALIFY ", exprList, qualify, qualifyIndex);
        if (sort != null) {
            builder.append("\nORDER BY ").append(
                    sort.getSQL(exprList, visibleColumnCount, alwaysQuote));
        }
        if (orderList != null) {
            builder.append("\nORDER BY ");
            for (int i = 0, l = orderList.size(); i < l; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                orderList.get(i).getSQL(builder, alwaysQuote);
            }
        }
        appendLimitToSQL(builder, alwaysQuote);
        if (sampleSizeExpr != null) {
            builder.append("\nSAMPLE_SIZE ");
            sampleSizeExpr.getUnenclosedSQL(builder, alwaysQuote);
        }
        if (isForUpdate) {
            builder.append("\nFOR UPDATE");
        }
        if (isQuickAggregateQuery) {
            builder.append("\n/* direct lookup */");
        }
        if (isDistinctQuery) {
            builder.append("\n/* distinct */");
        }
        if (sortUsingIndex) {
            builder.append("\n/* index sorted */");
        }
        if (isGroupQuery) {
            if (isGroupSortedQuery) {
                builder.append("\n/* group sorted */");
            }
        }
        // buff.append("\n/* cost: " + cost + " */");
        return builder.toString();
    }

    private static void getFilterSQL(StringBuilder builder, String sql, Expression[] exprList, Expression condition,
            int conditionIndex) {
        if (condition != null) {
            builder.append(sql);
            condition.getUnenclosedSQL(builder, true);
        } else if (conditionIndex >= 0) {
            builder.append(sql);
            exprList[conditionIndex].getUnenclosedSQL(builder, true);
        }
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public Expression getHaving() {
        return having;
    }

    public void setQualify(Expression qualify) {
        this.qualify = qualify;
    }

    public Expression getQualify() {
        return qualify;
    }

    @Override
    public int getColumnCount() {
        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public void setForUpdate(boolean b) {
        if (b && (isAnyDistinct() || isGroupQuery)) {
            throw DbException.get(ErrorCode.FOR_UPDATE_IS_NOT_ALLOWED_IN_DISTINCT_OR_GROUPED_SELECT);
        }
        this.isForUpdate = b;
        if (session.getDatabase().isMVStore()) {
            isForUpdateMvcc = b;
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level, Expression.MAP_INITIAL);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level, Expression.MAP_INITIAL);
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

    /**
     * Checks if this query is a group query.
     *
     * @return whether this query is a group query.
     */
    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    /**
     * Checks if this query contains window functions.
     *
     * @return whether this query contains window functions
     */
    public boolean isWindowQuery() {
        return isWindowQuery;
    }

    /**
     * Checks if window stage of group window query is performed. If true,
     * column resolver may not be used.
     *
     * @return true if window stage of group window query is performed
     */
    public boolean isGroupWindowStage2() {
        return isGroupWindowStage2;
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
        if (isWindowQuery) {
            if (qualify == null) {
                qualify = comp;
            } else {
                qualify = new ConditionAndOr(ConditionAndOr.AND, comp, qualify);
            }
            return;
        }
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
    public void updateAggregate(Session s, int stage) {
        for (Expression e : expressions) {
            e.updateAggregate(s, stage);
        }
        if (condition != null) {
            condition.updateAggregate(s, stage);
        }
        if (having != null) {
            having.updateAggregate(s, stage);
        }
        if (qualify != null) {
            qualify.updateAggregate(s, stage);
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
        for (Expression e : expressions) {
            if (!e.isEverything(v2)) {
                return false;
            }
        }
        if (condition != null && !condition.isEverything(v2)) {
            return false;
        }
        if (having != null && !having.isEverything(v2)) {
            return false;
        }
        if (qualify != null && !qualify.isEverything(v2)) {
            return false;
        }
        return true;
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
    public boolean allowGlobalConditions() {
        return offsetExpr == null && (limitExpr == null && distinctExpressions == null || sort == null);
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    /**
     * Lazy execution for this select.
     */
    private abstract class LazyResultSelect extends LazyResult {

        long rowNumber;
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

        private int sampleSize;

        private boolean forUpdate;

        LazyResultQueryFlat(Expression[] expressions, int columnCount, int sampleSize, boolean forUpdate) {
            super(expressions, columnCount);
            this.sampleSize = sampleSize;
            this.forUpdate = forUpdate;
        }

        @Override
        protected Value[] fetchNextRow() {
            while ((sampleSize <= 0 || rowNumber < sampleSize) && topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                // This method may lock rows
                if (forUpdate ? isConditionMetForUpdate() : isConditionMet()) {
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

        @Override
        protected boolean skipNextRow() {
            while ((sampleSize <= 0 || rowNumber < sampleSize) && topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                // This method does not lock rows
                if (isConditionMet()) {
                    ++rowNumber;
                    return true;
                }
            }
            return false;
        }

    }

    /**
     * Lazy execution for a group sorted query.
     */
    private final class LazyResultGroupSorted extends LazyResultSelect {

        private Value[] previousKeyValues;

        LazyResultGroupSorted(Expression[] expressions, int columnCount) {
            super(expressions, columnCount);
            if (groupData == null) {
                setGroupData(SelectGroups.getInstance(getSession(), Select.this.expressions, isGroupQuery,
                        groupIndex));
            } else {
                // TODO is this branch possible?
                updateAgg(columnCount, DataAnalysisOperation.STAGE_RESET);
                groupData.resetLazy();
            }
        }

        @Override
        public void reset() {
            super.reset();
            groupData.resetLazy();
            previousKeyValues = null;
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
                        groupData.nextLazyGroup();
                    } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                        row = createGroupSortedRow(previousKeyValues, columnCount);
                        previousKeyValues = keyValues;
                        groupData.nextLazyGroup();
                    }
                    groupData.nextLazyRow();
                    updateAgg(columnCount, DataAnalysisOperation.STAGE_GROUP);
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

    /**
     * Returns parent select, or null.
     *
     * @return parent select, or null
     */
    public Select getParentSelect() {
        return parentSelect;
    }

}
