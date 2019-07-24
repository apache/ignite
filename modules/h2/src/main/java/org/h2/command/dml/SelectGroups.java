/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.aggregate.AggregateData;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.PartitionData;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Grouped data for aggregates.
 *
 * <p>
 * Call sequence:
 * </p>
 * <ul>
 * <li>{@link #reset()}.</li>
 * <li>For each source row {@link #nextSource()} should be invoked.</li>
 * <li>{@link #done()}.</li>
 * <li>{@link #next()} is invoked inside a loop until it returns null.</li>
 * </ul>
 * <p>
 * Call sequence for lazy group sorted result:
 * </p>
 * <ul>
 * <li>{@link #resetLazy()} (not required before the first execution).</li>
 * <li>For each source group {@link #nextLazyGroup()} should be invoked.</li>
 * <li>For each source row {@link #nextLazyRow()} should be invoked. Each group
 * can have one or more rows.</li>
 * </ul>
 */
public abstract class SelectGroups {

    private static final class Grouped extends SelectGroups {

        private final int[] groupIndex;

        /**
         * Map of group-by key to group-by expression data e.g. AggregateData
         */
        private TreeMap<ValueRow, Object[]> groupByData;

        /**
         * Key into groupByData that produces currentGroupByExprData. Not used
         * in lazy mode.
         */
        private ValueRow currentGroupsKey;

        /**
         * Cursor for {@link #next()} method.
         */
        private Iterator<Entry<ValueRow, Object[]>> cursor;

        Grouped(Session session, ArrayList<Expression> expressions, int[] groupIndex) {
            super(session, expressions);
            this.groupIndex = groupIndex;
        }

        @Override
        public void reset() {
            super.reset();
            if (groupByData != null) {
                for (Object[] aggrs : groupByData.values())
                    cleanupAggregates(aggrs);
            }

            groupByData = new TreeMap<>(session.getDatabase().getCompareMode());
            currentGroupsKey = null;
            cursor = null;
        }

        @Override
        public void nextSource() {
            if (groupIndex == null) {
                currentGroupsKey = ValueRow.getEmpty();
            } else {
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }
                currentGroupsKey = ValueRow.get(keyValues);
            }
            Object[] values = groupByData.get(currentGroupsKey);
            if (values == null) {
                values = createRow();
                groupByData.put(currentGroupsKey, values);

                onGroupChanged(currentGroupsKey, null, values);
            }
            currentGroupByExprData = values;
            currentGroupRowId++;
        }

        @Override
        void updateCurrentGroupExprData() {
            // this can be null in lazy mode
            if (currentGroupsKey != null) {
                // since we changed the size of the array, update the object in
                // the groups map
                Object[] old = groupByData.put(currentGroupsKey, currentGroupByExprData);

                onGroupChanged(currentGroupsKey, old, currentGroupByExprData);
            }
        }

        @Override
        public void done() {
            super.done();
            if (groupIndex == null && groupByData.size() == 0) {
                groupByData.put(ValueRow.getEmpty(), createRow());
            }
            cursor = groupByData.entrySet().iterator();
        }

        /** Current cursor entry. */
        Map.Entry<ValueRow, Object[]> curEntry;

        @Override
        public ValueRow next() {
            if (cursor.hasNext()) {
                curEntry = cursor.next();
                currentGroupByExprData = curEntry.getValue();
                currentGroupRowId++;
                return curEntry.getKey();
            }
            curEntry = null;
            return null;
        }

        @Override
        public void remove() {
            cursor.remove();
            currentGroupByExprData = null;
            currentGroupRowId--;

            cleanupAggregates(curEntry.getValue());

            onGroupChanged(curEntry.getKey(), curEntry.getValue(), null);

            curEntry = null;
        }

        @Override
        public void resetLazy() {
            super.resetLazy();
            assert groupByData == null || !groupByData.containsKey(currentGroupsKey);
            currentGroupsKey = null;
        }
    }

    private static final class Plain extends SelectGroups {

        private ArrayList<Object[]> rows;

        /**
         * Cursor for {@link #next()} method.
         */
        private Iterator<Object[]> cursor;

        Plain(Session session, ArrayList<Expression> expressions) {
            super(session, expressions);
        }

        @Override
        public void reset() {
            super.reset();
            if (rows != null) {
                for (Object[] r : rows)
                    cleanupAggregates(r);
            }

            rows = new ArrayList<>();
            cursor = null;
        }

        @Override
        public void nextSource() {
            Object[] values = createRow();
            rows.add(values);
            currentGroupByExprData = values;
            currentGroupRowId++;

            onGroupChanged(null, null, currentGroupByExprData);
        }

        @Override
        void updateCurrentGroupExprData() {
            Object[] old = rows.set(rows.size() - 1, currentGroupByExprData);

            onGroupChanged(null, old, currentGroupByExprData);
        }

        @Override
        public void done() {
            super.done();
            cursor = rows.iterator();
        }

        @Override
        public ValueRow next() {
            if (cursor.hasNext()) {
                currentGroupByExprData = cursor.next();
                currentGroupRowId++;
                return ValueRow.getEmpty();
            }
            return null;
        }
    }

    /**
     * The database session.
     */
    final Session session;

    /**
     * The query's column list, including invisible expressions such as order by expressions.
     */
    final ArrayList<Expression> expressions;

    /**
     * The array of current group-by expression data e.g. AggregateData.
     */
    Object[] currentGroupByExprData;

    /**
     * Maps an expression object to an index, to use in accessing the Object[]
     * pointed to by groupByData.
     */
    private final HashMap<Expression, Integer> exprToIndexInGroupByData = new HashMap<>();

    /**
     * Maps an window expression object to its data.
     */
    private final HashMap<DataAnalysisOperation, PartitionData> windowData = new HashMap<>();

    /**
     * Maps an partitioned window expression object to its data.
     */
    private final HashMap<DataAnalysisOperation, TreeMap<Value, PartitionData>> windowPartitionData = new HashMap<>();

    /**
     * The id of the current group.
     */
    int currentGroupRowId;

    /**
     * Memory reserved in bytes.
     *
     * Note: Poison value '-1' means memory tracking is disabled.
     */
    long memReserved;

    /**
     * Creates new instance of grouped data.
     *
     * @param session
     *            the session
     * @param expressions
     *            the expressions
     * @param isGroupQuery
     *            is this query is a group query
     * @param groupIndex
     *            the indexes of group expressions, or null
     * @return new instance of the grouped data.
     */
    public static SelectGroups getInstance(Session session, ArrayList<Expression> expressions, boolean isGroupQuery,
        int[] groupIndex) {
        return isGroupQuery ? new Grouped(session, expressions, groupIndex) : new Plain(session, expressions);
    }

    SelectGroups(Session session, ArrayList<Expression> expressions) {
        this.session = session;
        this.expressions = expressions;

        if (session.queryMemoryTracker() == null)
            memReserved = -1;
    }

    /**
     * Is there currently a group-by active.
     *
     * @return {@code true} if there is currently a group-by active,
     *          otherwise returns {@code false}.
     */
    public boolean isCurrentGroup() {
        return currentGroupByExprData != null;
    }

    /**
     * Get the group-by data for the current group and the passed in expression.
     *
     * @param expr
     *            expression
     * @return expression data or null
     */
    public final Object getCurrentGroupExprData(Expression expr) {
        Integer index = exprToIndexInGroupByData.get(expr);
        if (index == null) {
            return null;
        }
        return currentGroupByExprData[index];
    }

    /**
     * Set the group-by data for the current group and the passed in expression.
     *
     * @param expr
     *            expression
     * @param obj
     *            expression data to set
     */
    public final void setCurrentGroupExprData(Expression expr, Object obj) {
        Integer index = exprToIndexInGroupByData.get(expr);
        if (index != null) {
            assert currentGroupByExprData[index] == null;
            currentGroupByExprData[index] = obj;
            return;
        }
        index = exprToIndexInGroupByData.size();
        exprToIndexInGroupByData.put(expr, index);
        if (index >= currentGroupByExprData.length) {
            currentGroupByExprData = Arrays.copyOf(currentGroupByExprData, currentGroupByExprData.length * 2);
            updateCurrentGroupExprData();
        }
        currentGroupByExprData[index] = obj;
    }

    /**
     * Creates new object arrays to holds group-by data.
     *
     * @return new object array to holds group-by data.
     */
    final Object[] createRow() {
        return new Object[Math.max(exprToIndexInGroupByData.size(), expressions.size())];
    }

    /**
     * Get the window data for the specified expression.
     *
     * @param expr
     *            expression
     * @param partitionKey
     *            a key of partition
     * @return expression data or null
     */
    public final PartitionData getWindowExprData(DataAnalysisOperation expr, Value partitionKey) {
        if (partitionKey == null) {
            return windowData.get(expr);
        } else {
            TreeMap<Value, PartitionData> map = windowPartitionData.get(expr);
            return map != null ? map.get(partitionKey) : null;
        }
    }

    /**
     * Set the window data for the specified expression.
     *
     * @param expr
     *            expression
     * @param partitionKey
     *            a key of partition
     * @param obj
     *            window expression data to set
     */
    public final void setWindowExprData(DataAnalysisOperation expr, Value partitionKey, PartitionData obj) {
        if (partitionKey == null) {
            Object old = windowData.put(expr, obj);
            assert old == null;
        } else {
            TreeMap<Value, PartitionData> map = windowPartitionData.get(expr);
            if (map == null) {
                map = new TreeMap<>(session.getDatabase().getCompareMode());
                windowPartitionData.put(expr, map);
            }
            map.put(partitionKey, obj);
        }
    }

    /**
     * Update group-by data specified by implementation.
     */
    abstract void updateCurrentGroupExprData();

    /**
     * Returns identity of the current row. Used by aggregates to check whether
     * they already processed this row or not.
     *
     * @return identity of the current row
     */
    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    /**
     * Resets this group data for reuse.
     */
    public void reset() {
        currentGroupByExprData = null;
        exprToIndexInGroupByData.clear();
        windowData.clear();
        windowPartitionData.clear();
        currentGroupRowId = 0;

        if (trackable()) {
            session.queryMemoryTracker().release(memReserved);

            memReserved = 0;
        }
    }

    /**
     * Invoked for each source row to evaluate group key and setup all necessary
     * data for aggregates.
     */
    public abstract void nextSource();

    /**
     * Invoked after all source rows are evaluated.
     */
    public void done() {
        currentGroupRowId = 0;
    }

    /**
     * Returns the key of the next group.
     *
     * @return the key of the next group, or null
     */
    public abstract ValueRow next();

    /**
     * Removes the data for the current key.
     *
     * @see #next()
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Resets this group data for reuse in lazy mode.
     */
    public void resetLazy() {
        currentGroupByExprData = null;
        currentGroupRowId = 0;
    }

    /**
     * @param aggrs Aggregates to cleanup.
     */
    void cleanupAggregates(Object[] aggrs) {
        if (aggrs == null || !trackable())
            return;

        for (Object agg : aggrs) {
            if (agg instanceof AggregateData)
                ((AggregateData)agg).cleanup(session);
        }
    }

    /**
     * Moves group data to the next group in lazy mode.
     */
    public void nextLazyGroup() {
        cleanupAggregates(currentGroupByExprData);

        currentGroupByExprData = new Object[Math.max(exprToIndexInGroupByData.size(), expressions.size())];
    }

    /**
     * Moves group data to the next row in lazy mode.
     */
    public void nextLazyRow() {
        currentGroupRowId++;
    }

    /**
     * Gets the query's column list, including invisible expressions
     * such as order by expressions.
     *
     * @return Expressions.
     */
    public ArrayList<Expression> expressions() {
        return expressions;
    }

    /**
     * Group result updated callback.
     *
     * @param groupKey Row key.
     * @param old Old row.
     * @param row New row.
     */
    protected void onGroupChanged(ValueRow groupKey, Object[] old, Object[] row) {
        if (!trackable())
            return;

        assert old != null || row != null;

        long size;

        // Group result changed.
        if (row != null && old != null)
            size = (row.length - old.length) * Constants.MEMORY_OBJECT;
        // New group added.
        else if (old == null) {
            size = groupKey != null ? groupKey.getMemory() : 0;
            size += Constants.MEMORY_ARRAY + row.length * Constants.MEMORY_OBJECT;
        }
        // Group removed.
        else {
            size = groupKey != null ? -groupKey.getMemory() : 0;
            size -= Constants.MEMORY_ARRAY + old.length * Constants.MEMORY_OBJECT;
        }

        if (size > 0)
            session.queryMemoryTracker().reserve(size);
        else
            session.queryMemoryTracker().release(-size);

        memReserved += size;
    }

    /**
     * @return {@code True} if memory tracker available, {@code False} otherwise.
     */
    boolean trackable() {
        assert memReserved == -1 || session.queryMemoryTracker() != null;

        return memReserved != -1;
    }
}
