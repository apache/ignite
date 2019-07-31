/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.h2.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.condition.Comparison;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.IndexHints;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Termporary index based on an in-memory hash map that is built on fly.
 */
public class HashJoinIndex extends BaseIndex {
    /** String constant for Hash join hint, index name etc.. */
    public static final String HASH_JOIN_IDX = "HASH_JOIN_IDX";

    /** Cursor. */
    private final IteratorCursor cur = new IteratorCursor();

    /** Hash table by column specified by colId. */
    private Map<Value, List<Row>> hashTbl;

    /** Hashed (key) columns info. */
    private HashColumn[] hashColumns;

    /** Index to fill hash table. */
    private Index fillFromIndex;

    /** Conditions. */
    private Set<ConditionChecker> condsCheckers;

    /** Filter index condition. */
    private ArrayList<IndexCondition> filterIdxCond;

    /** Memory reserved for index data. */
    private long memoryReserved;

    /**
     * @param tbl Table to build temporary hash join index.
     */
    public HashJoinIndex(Table tbl) {
        super(tbl, 0, HASH_JOIN_IDX,
            IndexColumn.wrap(tbl.getColumns()), IndexType.createUnique(false, true));
    }

    /**
     * @param cond Index condition to test.
     * @return {@code true} if the filter is used in a EQUI-JOIN.
     */
    public static boolean isEquiJoinCondition(IndexCondition cond) {
        if (cond.getExpression() == null || !cond.isEvaluatable())
            return false;

        int cmpType = cond.getCompareType();

        if (cmpType != Comparison.EQUAL && cmpType != Comparison.EQUAL_NULL_SAFE)
            return false;

        HashSet<DbObject> dependencies = new HashSet<>();

        ExpressionVisitor depsVisitor = ExpressionVisitor.getDependenciesVisitor(dependencies);

        cond.getExpression().isEverything(depsVisitor);

        return dependencies.size() == 1;
    }

    /**
     * @param ses Session.
     * @param tbl Source table to build hash map.
     * @return true if Hash JOIN index is applicable for specifid masks: there is EQUALITY for only one column.
     */
    public static boolean isApplicable(Session ses, Table tbl) {
        return tbl.getRowCountApproximation(ses) < ses.getHashJoinMaxTableSize();
    }

    /**
     * @param indexHints Index hints to check HASH JOIN enabled.
     * @return true if Hash JOIN index is applicable for specifid masks: there is EQUALITY for only one column.
     */
    public static boolean isEnableByHint(IndexHints indexHints) {
        return indexHints != null
                && indexHints.getAllowedIndexes() != null
                && indexHints.getAllowedIndexes().size() == 1
                && indexHints.getAllowedIndexes().contains(HASH_JOIN_IDX);
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw new UnsupportedOperationException("Runtime HASH_JOIN_IDX index doesn't support 'add'");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw new UnsupportedOperationException("Runtime HASH_JOIN_IDX index doesn't support 'remove'");
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return getRowCountApproximation(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation(Session session) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks,
        TableFilter[] filters, int filter, SortOrder sortOrder,
        AllColumnsForPlan allColumnsSet) {

        // Doesn't applicable for batched (distributed) join
        if (ses.isJoinBatchEnabled())
            return Long.MAX_VALUE;

        double cost = 0;

        for (Column column : columns) {
            int index = column.getColumnId();

            int mask = masks[index];

            if (mask != 0 && ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY)) {
                if (cost > 0) {
                    // Decrease cost for each equality condition to choose the mask with max EQ condition:
                    // e.g. SELECT A, B where A.id0=b.id0 AND A.id1=B.id1
                    // the mask with both condition must be win!
                    cost -= 2;
                }
                else {
                    // base cost of temporary hash index.
                    cost = 2 * columns.length + table.getRowCountApproximation(ses) / 1000;
                }
            }
        }

        return cost > 0 ? cost : Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

    /** {@inheritDoc} */
    @Override public boolean canScan() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        if (hashTbl == null)
            build(ses);

        Value key = hashKey(first);

        // Because index is used only for EQUI-JOIN
        if (key.containsNull())
            return Cursor.EMPTY;

        // If hash join is allied properly and first != last then join result is empty.
        // Don't use assert check here because the query:
        //      SELECT * A, B WHERE A.jid=B.jid AND N.jid = 5
        // produces this case when optimization: 'DbSettings.optimizeTwoEquals' is disabled.
        if (!hashKey(first).equals(hashKey(last)))
            return Cursor.EMPTY;

        List<Row> res = hashTbl.get(key);

        if (res == null)
            return Cursor.EMPTY;

        cur.open(res.iterator());

        return cur;
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        // Used by debug print before end of prepare phase.
        if (fillFromIndex == null)
            return "HASH_JOIN_IDX";

        StringBuilder builder = new StringBuilder("HASH_JOIN_IDX ");

        builder.append("[fillFromIndex=").append(fillFromIndex.getName());

        builder.append(", hashedCols=[");
        for (int i = 0; i < hashColumns.length; ++i) {
            if (i > 0)
                builder.append(", ");

            builder.append(table.getColumn(hashColumns[i].colId).getName());
        }
        builder.append("]");

        if (condsCheckers != null && !condsCheckers.isEmpty()) {
            builder.append(", filters=[");

            int cnt = 0;

            for (ConditionChecker c : condsCheckers) {
                if (cnt > 0)
                    builder.append(", ");

                builder.append(c.getSQL());

                cnt++;
            }
            builder.append("]");
        }
        builder.append("]");

        return builder.toString();
    }

    /**
     * @return {@code true} if the hash table has been built already.
     */
    public boolean isBuilt() {
        return hashTbl != null;
    }

    /**
     * @param ses Session.
     * @param indexConditions Index conditions to filter values when hash table is built.
     */
    public void prepare(Session ses, ArrayList<IndexCondition> indexConditions) {
        assert hashTbl == null;

        List<HashColumn> hashCols = new ArrayList<>();

        filterIdxCond = new ArrayList<>();

        for (IndexCondition idxCond : indexConditions) {
            if (isEquiJoinCondition(idxCond)) {
                int colType = idxCond.getColumn().getType().getValueType();
                int expType = idxCond.getExpression().getType().getValueType();

                int targetType = Value.getHigherOrder(expType, colType);

                HashColumn col = new HashColumn(idxCond.getColumn().getColumnId(), targetType);

                if (!hashCols.contains(col))
                    hashCols.add(col);
            }
            else if (!idxCond.isAlwaysFalse())
                filterIdxCond.add(idxCond);
        }

        assert !hashCols.isEmpty() : "The set of join columns is empty for table '" + table.getName() + '\'';

        hashColumns = hashCols.toArray(new HashColumn[0]);

        prepareFillFromIndex(ses);

        // Prepare filter condition.
        for (IndexCondition idxCond : filterIdxCond) {
            ConditionChecker checker = ConditionChecker.create(ses, idxCond);

            if (condsCheckers == null && checker != null)
                condsCheckers = new HashSet<>();

            if (checker != null)
                condsCheckers.add(checker);
        }
    }

    /**
     * @param ses Session.
     */
    private void prepareFillFromIndex(Session ses) {
        int masks[] = IndexCondition.createMasksForTable(table, filterIdxCond);

        if (!ses.isJoinBatchEnabled()) {
            PlanItem plan = table.getBestPlanItem(ses, masks, null, 0, null, null, false);

            fillFromIndex = plan.getIndex();
        }
        else
            fillFromIndex = table.getScanIndex(ses);
    }

    /**
     * @param ses Session.
     * @return Cursor to fill hash table.
     */
    private Cursor openCursorToFillHashTable(Session ses) {
        if (fillFromIndex.isFindUsingFullTableScan())
            return fillFromIndex.find(ses, null, null);

        SearchRow first = null;
        SearchRow last = null;

        boolean colIndexed[] = new boolean[table.getColumns().length];

        for (Column c : fillFromIndex.getColumns())
            colIndexed[c.getColumnId()] = true;

        for (IndexCondition condition : filterIdxCond) {
            // If index can perform only full table scan do not try to use it for regular
            // lookups, each such lookup will perform an own table scan.
            Column column = condition.getColumn();

            if (!colIndexed[column.getColumnId()])
                continue;

            if (condition.getCompareType() != Comparison.IN_LIST && condition.getCompareType() != Comparison.IN_QUERY) {
                Value v = condition.getCurrentValue(ses);

                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();

                int columnId = column.getColumnId();

                if (columnId != SearchRow.ROWID_INDEX) {
                    IndexColumn idxCol = indexColumns[columnId];
                    if (idxCol != null && (idxCol.sortType & SortOrder.DESCENDING) != 0) {
                        // if the index column is sorted the other way, we swap
                        // end and start NULLS_FIRST / NULLS_LAST is not a
                        // problem, as nulls never match anyway
                        boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                }

                if (isStart)
                    first = table.getSearchRow(first, columnId, v, true);

                if (isEnd)
                    last = table.getSearchRow(last, columnId, v, false);
            }
        }

        return fillFromIndex.find(ses, first, last);
    }

    /**
     * @param ses Session.
     */
    private void build(Session ses) {
        long t0 = System.currentTimeMillis();

        if (condsCheckers != null){
            for (ConditionChecker c : condsCheckers)
                c.calculateValue(ses);
        }

        Cursor cur = openCursorToFillHashTable(ses);

        hashTbl = new HashMap<>();

        // Don't use ignorecase on build.
        H2MemoryTracker memTracker = ses.queryMemoryTracker();

        while (cur.next()) {
            Row r = cur.get();

            if (checkConditions(ses, r)) {
                Value key = hashKey(r);

                // Because index is used only for EQUI-JOIN
                if (key.containsNull())
                    continue;

                List<Row> keyRows = hashTbl.get(key);

                if (memTracker != null) {
                    int size = keyRows != null ? 0 :
                        40 /*HashMap entry*/ + key.getMemory() + Constants.MEMORY_ARRAY;

                    size += Constants.MEMORY_POINTER + r.getMemory();

                    memTracker.reserve(size);
                    memoryReserved += size;
                }

                if (keyRows == null) {
                    keyRows = new ArrayList<>();

                    hashTbl.put(key, keyRows);
                }

                keyRows.add(r);
            }
        }

        Trace t = ses.getTrace();

        if (t.isDebugEnabled()) {
            t.debug("Build hash table for {0}, size={1}. Duration={2} ms",
                    table.getName(), hashTbl.size(), System.currentTimeMillis() - t0);
        }
    }

    /**
     * @param r Row.
     * @return Hash key.
     */
    private Value hashKey(SearchRow r) {
        if (hashColumns.length == 1)
            return r.getValue(hashColumns[0].colId).convertTo(hashColumns[0].targetType);

        Value[] key = new Value[hashColumns.length];

        for (int i = 0; i < hashColumns.length; ++i) {
            HashColumn col = hashColumns[i];

            key[i] = r.getValue(col.colId).convertTo(col.targetType);
        }

        return ValueArray.get(key);
    }

    /**
     * @param ses Session.
     * @param r Current row.
     * @return {@code true} if the row is OK with all index conditions.
     */
    private boolean checkConditions(Session ses, Row r) {
        if (condsCheckers != null) {
            for (ConditionChecker checker : condsCheckers) {
                if (!checker.check(getTable(), r))
                    return false;
            }
        }

        return true;
    }

    /**
     * @param session Session.
     */
    public void clearHashTable(Session session) {
        hashTbl = null;

        if (memoryReserved > 0) {
            assert session.queryMemoryTracker() != null;

            session.queryMemoryTracker().release(memoryReserved);

            memoryReserved = 0;
        }
    }

    /**
     *
     */
    private class IteratorCursor implements Cursor {
        /** Iterator. */
        private Iterator<Row> it;

        /** Current row. */
        private Row current;

        /**
         * @param it Iterator.
         */
        public void open(Iterator<Row> it) {
            this.it = it;
            current = null;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("prev");
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (it.hasNext()) {
                current = it.next();

                return true;
            }

            current = null;

            return false;
        }

        /** {@inheritDoc} */
        @Override public Row getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return current;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "IteratorCursor->" + current;
        }
    }

    /**
     *
     */
    private abstract static class ConditionChecker {
        /** Column ID. */
        int colId;

        /** Value to compare. */
        Value v;

        /** Value expr. */
        IndexCondition idxCond;

        /**
         * @param tbl Table to compare.
         * @param r Row to check condition.
         * @return {@code true} if row is OK with the condition.
         */
        boolean check(Table tbl, Row r) {
            Value o = r.getValue(colId);

            if (o.containsNull())
                return false;

            return checkValue(tbl, o);
        }

        /**
         * @param ses Session.
         */
        void calculateValue(Session ses) {
            v = idxCond.getCurrentValue(ses);
        }

        /**
         * @return SQL string.
         */
        public String getSQL() {
            return idxCond.getSQL(false);
        }

        /**
         * @param tbl Table to compare values.
         * @param o Value to compare.
         * @return Compare result.
         */
        abstract boolean checkValue(Table tbl, Value o);

        /**
         * @param ses Session.
         * @param idxCond Index condition to create checker.
         * @return Condition checker.
         */
        static ConditionChecker create(Session ses, IndexCondition idxCond) {
            ConditionChecker checker;

            switch (idxCond.getCompareType()) {
                case Comparison.IN_LIST:
                case Comparison.IN_QUERY:
                case Comparison.SPATIAL_INTERSECTS:
                case Comparison.FALSE:
                    return null;

                case Comparison.EQUAL:
                case Comparison.EQUAL_NULL_SAFE:
                    checker = new ConditionEqualChecker();
                    break;

                case Comparison.BIGGER_EQUAL:
                    checker = new ConditionBiggerEqualChecker();
                    break;

                case Comparison.BIGGER:
                    checker = new ConditionBiggerChecker();
                    break;

                case Comparison.SMALLER_EQUAL:
                    checker = new ConditionSmallerEqualChecker();
                    break;

                case Comparison.SMALLER:
                    checker = new ConditionSmallerChecker();
                    break;

                default:
                    throw DbException.throwInternalError("type=" + idxCond.getCompareType());
            }

            if (checker != null) {
                checker.colId = idxCond.getColumn().getColumnId();
                checker.idxCond = idxCond;
            }

            return checker;
        }
    }

    /**
     *
     */
    private static class ConditionEqualChecker extends ConditionChecker {
        /** {@inheritDoc} */
        @Override boolean checkValue(Table tbl, Value o) {
            return tbl.compareValues(o, v) == 0;
        }
    }

    /**
     *
     */
    private static class ConditionBiggerEqualChecker extends ConditionChecker {
        /** {@inheritDoc} */
        @Override boolean checkValue(Table tbl, Value o) {
            return tbl.compareValues(o, v) >= 0;
        }
    }

    /**
     *
     */
    private static class ConditionBiggerChecker extends ConditionChecker {
        /** {@inheritDoc} */
        @Override boolean checkValue(Table tbl, Value o) {
            return tbl.compareValues(o, v) > 0;
        }
    }

    /**
     *
     */
    private static class ConditionSmallerChecker extends ConditionChecker {
        /** {@inheritDoc} */
        @Override boolean checkValue(Table tbl, Value o) {
            return tbl.compareValues(o, v) < 0;
        }
    }

    /**
     *
     */
    private static class ConditionSmallerEqualChecker extends ConditionChecker {
        /** {@inheritDoc} */
        @Override boolean checkValue(Table tbl, Value o) {
            return tbl.compareValues(o, v) <= 0;
        }
    }

    /**
     *
     */
    private static class HashColumn {
        /** Column ID. */
        private final int colId;

        /** Target type. */
        private final int targetType;

        /**
         * @param colId Column ID
         * @param targetType target type.
         */
        private HashColumn(int colId, int targetType) {
            this.colId = colId;
            this.targetType = targetType;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            HashColumn column = (HashColumn)o;

            return colId == column.colId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return colId;
        }
    }
}
