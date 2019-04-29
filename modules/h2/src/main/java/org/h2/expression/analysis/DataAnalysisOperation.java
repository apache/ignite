/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * A base class for data analysis operations such as aggregates and window
 * functions.
 */
public abstract class DataAnalysisOperation extends Expression {

    /**
     * Reset stage. Used to reset internal data to its initial state.
     */
    public static final int STAGE_RESET = 0;

    /**
     * Group stage, used for explicit or implicit GROUP BY operation.
     */
    public static final int STAGE_GROUP = 1;

    /**
     * Window processing stage.
     */
    public static final int STAGE_WINDOW = 2;

    /**
     * SELECT
     */
    protected final Select select;

    /**
     * OVER clause
     */
    protected Window over;

    /**
     * Sort order for OVER
     */
    protected SortOrder overOrderBySort;

    private int numFrameExpressions;

    private int lastGroupRowId;

    /**
     * Create sort order.
     *
     * @param session
     *            database session
     * @param orderBy
     *            array of order by expressions
     * @param offset
     *            index offset
     * @return the SortOrder
     */
    protected static SortOrder createOrder(Session session, ArrayList<SelectOrderBy> orderBy, int offset) {
        int size = orderBy.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = orderBy.get(i);
            index[i] = i + offset;
            sortType[i] = o.sortType;
        }
        return new SortOrder(session.getDatabase(), index, sortType, null);
    }

    protected DataAnalysisOperation(Select select) {
        this.select = select;
    }

    /**
     * Sets the OVER condition.
     *
     * @param over
     *            OVER condition
     */
    public void setOverCondition(Window over) {
        this.over = over;
    }

    /**
     * Checks whether this expression is an aggregate function.
     *
     * @return true if this is an aggregate function (including aggregates with
     *         OVER clause), false if this is a window function
     */
    public abstract boolean isAggregate();

    /**
     * Returns the sort order for OVER clause.
     *
     * @return the sort order for OVER clause
     */
    protected SortOrder getOverOrderBySort() {
        return overOrderBySort;
    }

    @Override
    public final void mapColumns(ColumnResolver resolver, int level, int state) {
        if (over != null) {
            if (state != MAP_INITIAL) {
                throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL(false));
            }
            state = MAP_IN_WINDOW;
        } else {
            if (state == MAP_IN_AGGREGATE) {
                throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL(false));
            }
            state = MAP_IN_AGGREGATE;
        }
        mapColumnsAnalysis(resolver, level, state);
    }

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver
     *            the column resolver
     * @param level
     *            the subquery nesting level
     * @param innerState
     *            one of the Expression MAP_IN_* values
     */
    protected void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (over != null) {
            over.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(Session session) {
        if (over != null) {
            over.optimize(session);
            ArrayList<SelectOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                overOrderBySort = createOrder(session, orderBy, getNumExpressions());
            } else if (!isAggregate()) {
                overOrderBySort = new SortOrder(session.getDatabase(), new int[getNumExpressions()], new int[0], null);
            }
            WindowFrame frame = over.getWindowFrame();
            if (frame != null) {
                int index = getNumExpressions();
                int orderBySize = 0;
                if (orderBy != null) {
                    orderBySize = orderBy.size();
                    index += orderBySize;
                }
                int n = 0;
                WindowFrameBound bound = frame.getStarting();
                if (bound.isParameterized()) {
                    if (orderBySize != 1) {
                        throw getSingleSortKeyException();
                    }
                    if (bound.isVariable()) {
                        bound.setExpressionIndex(index);
                        n++;
                    }
                }
                bound = frame.getFollowing();
                if (bound != null && bound.isParameterized()) {
                    if (orderBySize != 1) {
                        throw getSingleSortKeyException();
                    }
                    if (bound.isVariable()) {
                        bound.setExpressionIndex(index + n);
                        n++;
                    }
                }
                numFrameExpressions = n;
            }
        }
        return this;
    }

    private DbException getSingleSortKeyException() {
        String sql = getSQL(false);
        return DbException.getSyntaxError(sql, sql.length() - 1, "exactly one sort key is required for RANGE units");
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (over != null) {
            over.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public final void updateAggregate(Session session, int stage) {
        if (stage == STAGE_RESET) {
            updateGroupAggregates(session, STAGE_RESET);
            lastGroupRowId = 0;
            return;
        }
        boolean window = stage == STAGE_WINDOW;
        if (window != (over != null)) {
            if (!window && select.isWindowQuery()) {
                updateGroupAggregates(session, stage);
            }
            return;
        }
        SelectGroups groupData = select.getGroupDataIfCurrent(window);
        if (groupData == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = groupData.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        if (over != null) {
            if (!select.isGroupQuery()) {
                over.updateAggregate(session, stage);
            }
        }
        updateAggregate(session, groupData, groupRowId);
    }

    /**
     * Update a row of an aggregate.
     *
     * @param session
     *            the database session
     * @param groupData
     *            data for the aggregate group
     * @param groupRowId
     *            row id of group
     */
    protected abstract void updateAggregate(Session session, SelectGroups groupData, int groupRowId);

    /**
     * Invoked when processing group stage of grouped window queries to update
     * arguments of this aggregate.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     */
    protected void updateGroupAggregates(Session session, int stage) {
        if (over != null) {
            over.updateAggregate(session, stage);
        }
    }

    /**
     * Returns the number of expressions, excluding OVER clause.
     *
     * @return the number of expressions
     */
    protected abstract int getNumExpressions();

    /**
     * Returns the number of window frame expressions.
     *
     * @return the number of window frame expressions
     */
    private int getNumFrameExpressions() {
        return numFrameExpressions;
    }

    /**
     * Stores current values of expressions into the specified array.
     *
     * @param session
     *            the session
     * @param array
     *            array to store values of expressions
     */
    protected abstract void rememberExpressions(Session session, Value[] array);

    /**
     * Get the aggregate data for a window clause.
     *
     * @param session
     *            database session
     * @param groupData
     *            aggregate group data
     * @param forOrderBy
     *            true if this is for ORDER BY
     * @return the aggregate data object, specific to each kind of aggregate.
     */
    protected Object getWindowData(Session session, SelectGroups groupData, boolean forOrderBy) {
        Object data;
        Value key = over.getCurrentKey(session);
        PartitionData partition = groupData.getWindowExprData(this, key);
        if (partition == null) {
            data = forOrderBy ? new ArrayList<>() : createAggregateData();
            groupData.setWindowExprData(this, key, new PartitionData(data));
        } else {
            data = partition.getData();
        }
        return data;
    }

    /**
     * Get the aggregate group data object from the collector object.
     *
     * @param groupData
     *            the collector object
     * @param ifExists
     *            if true, return null if object not found, if false, return new
     *            object if nothing found
     * @return group data object
     */
    protected Object getGroupData(SelectGroups groupData, boolean ifExists) {
        Object data;
        data = groupData.getCurrentGroupExprData(this);
        if (data == null) {
            if (ifExists) {
                return null;
            }
            data = createAggregateData();
            groupData.setCurrentGroupExprData(this, data);
        }
        return data;
    }

    /**
     * Create aggregate data object specific to the subclass.
     *
     * @return aggregate-specific data object.
     */
    protected abstract Object createAggregateData();

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (over == null) {
            return true;
        }
        switch (visitor.getType()) {
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
            return false;
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.GET_COLUMNS1:
        case ExpressionVisitor.GET_COLUMNS2:
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public Value getValue(Session session) {
        SelectGroups groupData = select.getGroupDataIfCurrent(over != null);
        if (groupData == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL(false));
        }
        return over == null ? getAggregatedValue(session, getGroupData(groupData, true))
                : getWindowResult(session, groupData);
    }

    /**
     * Returns result of this window function or window aggregate. This method
     * is not used for plain aggregates.
     *
     * @param session
     *            the session
     * @param groupData
     *            the group data
     * @return result of this function
     */
    private Value getWindowResult(Session session, SelectGroups groupData) {
        PartitionData partition;
        Object data;
        boolean forOrderBy = over.getOrderBy() != null;
        Value key = over.getCurrentKey(session);
        partition = groupData.getWindowExprData(this, key);
        if (partition == null) {
            // Window aggregates with FILTER clause may have no collected values
            data = forOrderBy ? new ArrayList<>() : createAggregateData();
            partition = new PartitionData(data);
            groupData.setWindowExprData(this, key, partition);
        } else {
            data = partition.getData();
        }
        if (forOrderBy || !isAggregate()) {
            Value result = getOrderedResult(session, groupData, partition, data);
            if (result == null) {
                return getAggregatedValue(session, null);
            }
            return result;
        }
        // Window aggregate without ORDER BY clause in window specification
        Value result = partition.getResult();
        if (result == null) {
            result = getAggregatedValue(session, data);
            partition.setResult(result);
        }
        return result;
    }

    /***
     * Returns aggregated value.
     *
     * @param session
     *            the session
     * @param aggregateData
     *            the aggregate data
     * @return aggregated value.
     */
    protected abstract Value getAggregatedValue(Session session, Object aggregateData);

    /**
     * Update a row of an ordered aggregate.
     *
     * @param session
     *            the database session
     * @param groupData
     *            data for the aggregate group
     * @param groupRowId
     *            row id of group
     * @param orderBy
     *            list of order by expressions
     */
    protected void updateOrderedAggregate(Session session, SelectGroups groupData, int groupRowId,
            ArrayList<SelectOrderBy> orderBy) {
        int ne = getNumExpressions();
        int size = orderBy != null ? orderBy.size() : 0;
        int frameSize = getNumFrameExpressions();
        Value[] array = new Value[ne + size + frameSize + 1];
        rememberExpressions(session, array);
        for (int i = 0; i < size; i++) {
            @SuppressWarnings("null")
            SelectOrderBy o = orderBy.get(i);
            array[ne++] = o.expression.getValue(session);
        }
        if (frameSize > 0) {
            WindowFrame frame = over.getWindowFrame();
            WindowFrameBound bound = frame.getStarting();
            if (bound.isVariable()) {
                array[ne++] = bound.getValue().getValue(session);
            }
            bound = frame.getFollowing();
            if (bound != null && bound.isVariable()) {
                array[ne++] = bound.getValue().getValue(session);
            }
        }
        array[ne] = ValueInt.get(groupRowId);
        @SuppressWarnings("unchecked")
        ArrayList<Value[]> data = (ArrayList<Value[]>) getWindowData(session, groupData, true);
        data.add(array);
    }

    private Value getOrderedResult(Session session, SelectGroups groupData, PartitionData partition, Object data) {
        HashMap<Integer, Value> result = partition.getOrderedResult();
        if (result == null) {
            result = new HashMap<>();
            @SuppressWarnings("unchecked")
            ArrayList<Value[]> orderedData = (ArrayList<Value[]>) data;
            int rowIdColumn = getNumExpressions();
            ArrayList<SelectOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                rowIdColumn += orderBy.size();
                Collections.sort(orderedData, overOrderBySort);
            }
            rowIdColumn += getNumFrameExpressions();
            getOrderedResultLoop(session, result, orderedData, rowIdColumn);
            partition.setOrderedResult(result);
        }
        return result.get(groupData.getCurrentGroupRowId());
    }

    /**
     * Returns result of this window function or window aggregate. This method
     * may not be called on window aggregate without window order clause.
     *
     * @param session
     *            the session
     * @param result
     *            the map to append result to
     * @param ordered
     *            ordered data
     * @param rowIdColumn
     *            the index of row id value
     */
    protected abstract void getOrderedResultLoop(Session session, HashMap<Integer, Value> result,
            ArrayList<Value[]> ordered, int rowIdColumn);

    /**
     * Used to create SQL for the OVER and FILTER clauses.
     *
     * @param builder
     *            string builder
     * @param alwaysQuote
     *            quote all identifiers
     * @return the builder object
     */
    protected StringBuilder appendTailConditions(StringBuilder builder, boolean alwaysQuote) {
        if (over != null) {
            builder.append(' ');
            over.getSQL(builder, alwaysQuote);
        }
        return builder;
    }

}
