/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexCursor;
import org.h2.index.IndexLookupBatch;
import org.h2.index.ViewCursor;
import org.h2.index.ViewIndex;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.util.DoneFuture;
import org.h2.util.LazyFuture;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueLong;

/**
 * Support for asynchronous batched index lookups on joins.
 *
 * @see BaseIndex#createLookupBatch(org.h2.table.TableFilter[], int)
 * @see IndexLookupBatch
 * @author Sergi Vladykin
 */
public final class JoinBatch {

    /**
     * An empty cursor.
     */
    static final Cursor EMPTY_CURSOR = new Cursor() {
        @Override
        public boolean previous() {
            return false;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public SearchRow getSearchRow() {
            return null;
        }

        @Override
        public Row get() {
            return null;
        }

        @Override
        public String toString() {
            return "EMPTY_CURSOR";
        }
    };

    /**
     * An empty future cursor.
     */
    static final Future<Cursor> EMPTY_FUTURE_CURSOR = new DoneFuture<>(EMPTY_CURSOR);

    /**
     * The top cursor.
     */
    Future<Cursor> viewTopFutureCursor;

    /**
     * The top filter.
     */
    JoinFilter top;

    /**
     * The filters.
     */
    final JoinFilter[] filters;

    /**
     * Whether this is a batched subquery.
     */
    boolean batchedSubQuery;

    private boolean started;

    private JoinRow current;
    private boolean found;

    /**
     * This filter joined after this batched join and can be used normally.
     */
    private final TableFilter additionalFilter;

    /**
     * @param filtersCount number of filters participating in this batched join
     * @param additionalFilter table filter after this batched join.
     */
    public JoinBatch(int filtersCount, TableFilter additionalFilter) {
        if (filtersCount > 32) {
            // This is because we store state in a 64 bit field, 2 bits per
            // joined table.
            throw DbException.getUnsupportedException(
                    "Too many tables in join (at most 32 supported).");
        }
        filters = new JoinFilter[filtersCount];
        this.additionalFilter = additionalFilter;
    }

    /**
     * Get the lookup batch for the given table filter.
     *
     * @param joinFilterId joined table filter id
     * @return lookup batch
     */
    public IndexLookupBatch getLookupBatch(int joinFilterId) {
        return filters[joinFilterId].lookupBatch;
    }

    /**
     * Reset state of this batch.
     *
     * @param beforeQuery {@code true} if reset was called before the query run,
     *            {@code false} if after
     */
    public void reset(boolean beforeQuery) {
        current = null;
        started = false;
        found = false;
        for (JoinFilter jf : filters) {
            jf.reset(beforeQuery);
        }
        if (beforeQuery && additionalFilter != null) {
            additionalFilter.reset();
        }
    }

    /**
     * Register the table filter and lookup batch.
     *
     * @param filter table filter
     * @param lookupBatch lookup batch
     */
    public void register(TableFilter filter, IndexLookupBatch lookupBatch) {
        assert filter != null;
        top = new JoinFilter(lookupBatch, filter, top);
        filters[top.id] = top;
    }

    /**
     * Get the value for the given column.
     *
     * @param filterId table filter id
     * @param column the column
     * @return column value for current row
     */
    public Value getValue(int filterId, Column column) {
        if (current == null) {
            return null;
        }
        Object x = current.row(filterId);
        assert x != null;
        Row row = current.isRow(filterId) ? (Row) x : ((Cursor) x).get();
        int columnId = column.getColumnId();
        if (columnId == -1) {
            return ValueLong.get(row.getKey());
        }
        Value value = row.getValue(column.getColumnId());
        if (value == null) {
            throw DbException.throwInternalError("value is null: " + column + " " + row);
        }
        return value;
    }

    private void start() {
        // initialize current row
        current = new JoinRow(new Object[filters.length]);
        // initialize top cursor
        Cursor cursor;
        if (batchedSubQuery) {
            assert viewTopFutureCursor != null;
            cursor = get(viewTopFutureCursor);
        } else {
            // setup usual index cursor
            TableFilter f = top.filter;
            IndexCursor indexCursor = f.getIndexCursor();
            indexCursor.find(f.getSession(), f.getIndexConditions());
            cursor = indexCursor;
        }
        current.updateRow(top.id, cursor, JoinRow.S_NULL, JoinRow.S_CURSOR);
        // we need fake first row because batchedNext always will move to the
        // next row
        JoinRow fake = new JoinRow(null);
        fake.next = current;
        current = fake;
    }

    /**
     * Get next row from the join batch.
     *
     * @return true if there is a next row
     */
    public boolean next() {
        if (!started) {
            start();
            started = true;
        }
        if (additionalFilter == null) {
            if (batchedNext()) {
                assert current.isComplete();
                return true;
            }
            return false;
        }
        while (true) {
            if (!found) {
                if (!batchedNext()) {
                    return false;
                }
                assert current.isComplete();
                found = true;
                additionalFilter.reset();
            }
            // we call furtherFilter in usual way outside of this batch because
            // it is more effective
            if (additionalFilter.next()) {
                return true;
            }
            found = false;
        }
    }

    private static Cursor get(Future<Cursor> f) {
        Cursor c;
        try {
            c = f.get();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return c == null ? EMPTY_CURSOR : c;
    }

    private boolean batchedNext() {
        if (current == null) {
            // after last
            return false;
        }
        // go next
        current = current.next;
        if (current == null) {
            return false;
        }
        current.prev = null;

        final int lastJfId = filters.length - 1;

        int jfId = lastJfId;
        while (current.row(jfId) == null) {
            // lookup for the first non fetched filter for the current row
            jfId--;
        }

        while (true) {
            fetchCurrent(jfId);

            if (!current.isDropped()) {
                // if current was not dropped then it must be fetched
                // successfully
                if (jfId == lastJfId) {
                    // the whole join row is ready to be returned
                    return true;
                }
                JoinFilter join = filters[jfId + 1];
                if (join.isBatchFull()) {
                    // get future cursors for join and go right to fetch them
                    current = join.find(current);
                }
                if (current.row(join.id) != null) {
                    // either find called or outer join with null-row
                    jfId = join.id;
                    continue;
                }
            }
            // we have to go down and fetch next cursors for jfId if it is
            // possible
            if (current.next == null) {
                // either dropped or null-row
                if (current.isDropped()) {
                    current = current.prev;
                    if (current == null) {
                        return false;
                    }
                }
                assert !current.isDropped();
                assert jfId != lastJfId;

                jfId = 0;
                while (current.row(jfId) != null) {
                    jfId++;
                }
                // force find on half filled batch (there must be either
                // searchRows or Cursor.EMPTY set for null-rows)
                current = filters[jfId].find(current);
            } else {
                // here we don't care if the current was dropped
                current = current.next;
                assert !current.isRow(jfId);
                while (current.row(jfId) == null) {
                    assert jfId != top.id;
                    // need to go left and fetch more search rows
                    jfId--;
                    assert !current.isRow(jfId);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void fetchCurrent(final int jfId) {
        assert current.prev == null || current.prev.isRow(jfId) : "prev must be already fetched";
        assert jfId == 0 || current.isRow(jfId - 1) : "left must be already fetched";

        assert !current.isRow(jfId) : "double fetching";

        Object x = current.row(jfId);
        assert x != null : "x null";

        // in case of outer join we don't have any future around empty cursor
        boolean newCursor = x == EMPTY_CURSOR;

        if (newCursor) {
            if (jfId == 0) {
                // the top cursor is new and empty, then the whole select will
                // not produce any rows
                current.drop();
                return;
            }
        } else if (current.isFuture(jfId)) {
            // get cursor from a future
            x = get((Future<Cursor>) x);
            current.updateRow(jfId, x, JoinRow.S_FUTURE, JoinRow.S_CURSOR);
            newCursor = true;
        }

        final JoinFilter jf = filters[jfId];
        Cursor c = (Cursor) x;
        assert c != null;
        JoinFilter join = jf.join;

        while (true) {
            if (c == null || !c.next()) {
                if (newCursor && jf.isOuterJoin()) {
                    // replace cursor with null-row
                    current.updateRow(jfId, jf.getNullRow(), JoinRow.S_CURSOR, JoinRow.S_ROW);
                    c = null;
                    newCursor = false;
                } else {
                    // cursor is done, drop it
                    current.drop();
                    return;
                }
            }
            if (!jf.isOk(c == null)) {
                // try another row from the cursor
                continue;
            }
            boolean joinEmpty = false;
            if (join != null && !join.collectSearchRows()) {
                if (join.isOuterJoin()) {
                    joinEmpty = true;
                } else {
                    // join will fail, try next row in the cursor
                    continue;
                }
            }
            if (c != null) {
                current = current.copyBehind(jfId);
                // update jf, set current row from cursor
                current.updateRow(jfId, c.get(), JoinRow.S_CURSOR, JoinRow.S_ROW);
            }
            if (joinEmpty) {
                // update jf.join, set an empty cursor
                current.updateRow(join.id, EMPTY_CURSOR, JoinRow.S_NULL, JoinRow.S_CURSOR);
            }
            return;
        }
    }

    /**
     * @return Adapter to allow joining to this batch in sub-queries and views.
     */
    private IndexLookupBatch viewIndexLookupBatch(ViewIndex viewIndex) {
        return new ViewIndexLookupBatch(viewIndex);
    }

    /**
     * Create index lookup batch for a view index.
     *
     * @param viewIndex view index
     * @return index lookup batch or {@code null} if batching is not supported
     *         for this query
     */
    public static IndexLookupBatch createViewIndexLookupBatch(ViewIndex viewIndex) {
        Query query = viewIndex.getQuery();
        if (query.isUnion()) {
            ViewIndexLookupBatchUnion unionBatch = new ViewIndexLookupBatchUnion(viewIndex);
            return unionBatch.initialize() ? unionBatch : null;
        }
        JoinBatch jb = ((Select) query).getJoinBatch();
        if (jb == null || jb.getLookupBatch(0) == null) {
            // our sub-query is not batched or is top batched sub-query
            return null;
        }
        assert !jb.batchedSubQuery;
        jb.batchedSubQuery = true;
        return jb.viewIndexLookupBatch(viewIndex);
    }

    /**
     * Create fake index lookup batch for non-batched table filter.
     *
     * @param filter the table filter
     * @return fake index lookup batch
     */
    public static IndexLookupBatch createFakeIndexLookupBatch(TableFilter filter) {
        return new FakeLookupBatch(filter);
    }

    @Override
    public String toString() {
        return "JoinBatch->\n" + "prev->" + (current == null ? null : current.prev) +
                "\n" + "curr->" + current +
                "\n" + "next->" + (current == null ? null : current.next);
    }

    /**
     * Table filter participating in batched join.
     */
    private static final class JoinFilter {
        final IndexLookupBatch lookupBatch;
        final int id;
        final JoinFilter join;
        final TableFilter filter;

        JoinFilter(IndexLookupBatch lookupBatch, TableFilter filter, JoinFilter join) {
            this.filter = filter;
            this.id = filter.getJoinFilterId();
            this.join = join;
            this.lookupBatch = lookupBatch;
            assert lookupBatch != null || id == 0;
        }

        void reset(boolean beforeQuery) {
            if (lookupBatch != null) {
                lookupBatch.reset(beforeQuery);
            }
        }

        Row getNullRow() {
            return filter.getTable().getNullRow();
        }

        boolean isOuterJoin() {
            return filter.isJoinOuter();
        }

        boolean isBatchFull() {
            return lookupBatch.isBatchFull();
        }

        boolean isOk(boolean ignoreJoinCondition) {
            boolean filterOk = filter.isOk(filter.getFilterCondition());
            boolean joinOk = filter.isOk(filter.getJoinCondition());

            return filterOk && (ignoreJoinCondition || joinOk);
        }

        boolean collectSearchRows() {
            assert !isBatchFull();
            IndexCursor c = filter.getIndexCursor();
            c.prepare(filter.getSession(), filter.getIndexConditions());
            if (c.isAlwaysFalse()) {
                return false;
            }
            return lookupBatch.addSearchRows(c.getStart(), c.getEnd());
        }

        List<Future<Cursor>> find() {
            return lookupBatch.find();
        }

        JoinRow find(JoinRow current) {
            assert current != null;

            // lookupBatch is allowed to be empty when we have some null-rows
            // and forced find call
            List<Future<Cursor>> result = lookupBatch.find();

            // go backwards and assign futures
            for (int i = result.size(); i > 0;) {
                assert current.isRow(id - 1);
                if (current.row(id) == EMPTY_CURSOR) {
                    // outer join support - skip row with existing empty cursor
                    current = current.prev;
                    continue;
                }
                assert current.row(id) == null;
                Future<Cursor> future = result.get(--i);
                if (future == null) {
                    current.updateRow(id, EMPTY_CURSOR, JoinRow.S_NULL, JoinRow.S_CURSOR);
                } else {
                    current.updateRow(id, future, JoinRow.S_NULL, JoinRow.S_FUTURE);
                }
                if (current.prev == null || i == 0) {
                    break;
                }
                current = current.prev;
            }

            // handle empty cursors (because of outer joins) at the beginning
            while (current.prev != null && current.prev.row(id) == EMPTY_CURSOR) {
                current = current.prev;
            }
            assert current.prev == null || current.prev.isRow(id);
            assert current.row(id) != null;
            assert !current.isRow(id);

            // the last updated row
            return current;
        }

        @Override
        public String toString() {
            return "JoinFilter->" + filter;
        }
    }

    /**
     * Linked row in batched join.
     */
    private static final class JoinRow {
        private static final long S_NULL = 0;
        private static final long S_FUTURE = 1;
        private static final long S_CURSOR = 2;
        private static final long S_ROW = 3;

        private static final long S_MASK = 3;

        JoinRow prev;
        JoinRow next;

        /**
         * May contain one of the following:
         * <ul>
         * <li>{@code null}: means that we need to get future cursor
         * for this row</li>
         * <li>{@link Future}: means that we need to get a new {@link Cursor}
         * from the {@link Future}</li>
         * <li>{@link Cursor}: means that we need to fetch {@link Row}s from the
         * {@link Cursor}</li>
         * <li>{@link Row}: the {@link Row} is already fetched and is ready to
         * be used</li>
         * </ul>
         */
        private Object[] row;
        private long state;

        /**
         * @param row Row.
         */
        JoinRow(Object[] row) {
            this.row = row;
        }

        /**
         * @param joinFilterId Join filter id.
         * @return Row state.
         */
        private long getState(int joinFilterId) {
            return (state >>> (joinFilterId << 1)) & S_MASK;
        }

        /**
         * Allows to do a state transition in the following order:
         * 0. Slot contains {@code null} ({@link #S_NULL}).
         * 1. Slot contains {@link Future} ({@link #S_FUTURE}).
         * 2. Slot contains {@link Cursor} ({@link #S_CURSOR}).
         * 3. Slot contains {@link Row} ({@link #S_ROW}).
         *
         * @param joinFilterId {@link JoinRow} filter id.
         * @param i Increment by this number of moves.
         */
        private void incrementState(int joinFilterId, long i) {
            assert i > 0 : i;
            state += i << (joinFilterId << 1);
        }

        void updateRow(int joinFilterId, Object x, long oldState, long newState) {
            assert getState(joinFilterId) == oldState : "old state: " + getState(joinFilterId);
            row[joinFilterId] = x;
            incrementState(joinFilterId, newState - oldState);
            assert getState(joinFilterId) == newState : "new state: " + getState(joinFilterId);
        }

        Object row(int joinFilterId) {
            return row[joinFilterId];
        }

        boolean isRow(int joinFilterId) {
            return getState(joinFilterId) == S_ROW;
        }

        boolean isFuture(int joinFilterId) {
            return getState(joinFilterId) == S_FUTURE;
        }

        private boolean isCursor(int joinFilterId) {
            return getState(joinFilterId) == S_CURSOR;
        }

        boolean isComplete() {
            return isRow(row.length - 1);
        }

        boolean isDropped() {
            return row == null;
        }

        void drop() {
            if (prev != null) {
                prev.next = next;
            }
            if (next != null) {
                next.prev = prev;
            }
            row = null;
        }

        /**
         * Copy this JoinRow behind itself in linked list of all in progress
         * rows.
         *
         * @param jfId The last fetched filter id.
         * @return The copy.
         */
        JoinRow copyBehind(int jfId) {
            assert isCursor(jfId);
            assert jfId + 1 == row.length || row[jfId + 1] == null;

            Object[] r = new Object[row.length];
            if (jfId != 0) {
                System.arraycopy(row, 0, r, 0, jfId);
            }
            JoinRow copy = new JoinRow(r);
            copy.state = state;

            if (prev != null) {
                copy.prev = prev;
                prev.next = copy;
            }
            prev = copy;
            copy.next = this;

            return copy;
        }

        @Override
        public String toString() {
            return "JoinRow->" + Arrays.toString(row);
        }
    }

    /**
     * Fake Lookup batch for indexes which do not support batching but have to
     * participate in batched joins.
     */
    private static final class FakeLookupBatch implements IndexLookupBatch {
        private final TableFilter filter;

        private SearchRow first;
        private SearchRow last;

        private boolean full;

        private final List<Future<Cursor>> result = new SingletonList<>();

        FakeLookupBatch(TableFilter filter) {
            this.filter = filter;
        }

        @Override
        public String getPlanSQL() {
            return "fake";
        }

        @Override
        public void reset(boolean beforeQuery) {
            full = false;
            first = last = null;
            result.set(0, null);
        }

        @Override
        public boolean addSearchRows(SearchRow first, SearchRow last) {
            assert !full;
            this.first = first;
            this.last = last;
            full = true;
            return true;
        }

        @Override
        public boolean isBatchFull() {
            return full;
        }

        @Override
        public List<Future<Cursor>> find() {
            if (!full) {
                return Collections.emptyList();
            }
            Cursor c = filter.getIndex().find(filter, first, last);
            result.set(0, new DoneFuture<>(c));
            full = false;
            first = last = null;
            return result;
        }
    }

    /**
     * Simple singleton list.
     * @param <E> Element type.
     */
    static final class SingletonList<E> extends AbstractList<E> {
        private E element;

        @Override
        public E get(int index) {
            assert index == 0;
            return element;
        }

        @Override
        public E set(int index, E element) {
            assert index == 0;
            this.element = element;
            return null;
        }

        @Override
        public int size() {
            return 1;
        }
    }

    /**
     * Base class for SELECT and SELECT UNION view index lookup batches.
     * @param <R> Runner type.
     */
    private abstract static class ViewIndexLookupBatchBase<R extends QueryRunnerBase>
            implements IndexLookupBatch {
        protected final ViewIndex viewIndex;
        private final ArrayList<Future<Cursor>> result = New.arrayList();
        private int resultSize;
        private boolean findCalled;

        protected ViewIndexLookupBatchBase(ViewIndex viewIndex) {
            this.viewIndex = viewIndex;
        }

        @Override
        public String getPlanSQL() {
            return "view";
        }

        protected abstract boolean collectSearchRows(R r);

        protected abstract R newQueryRunner();

        protected abstract void startQueryRunners(int resultSize);

        protected final boolean resetAfterFind() {
            if (!findCalled) {
                return false;
            }
            findCalled = false;
            // method find was called, we need to reset futures to initial state
            // for reuse
            for (int i = 0; i < resultSize; i++) {
                queryRunner(i).reset();
            }
            resultSize = 0;
            return true;
        }

        @SuppressWarnings("unchecked")
        protected R queryRunner(int i) {
            return (R) result.get(i);
        }

        @Override
        public final boolean addSearchRows(SearchRow first, SearchRow last) {
            resetAfterFind();
            viewIndex.setupQueryParameters(viewIndex.getSession(), first, last, null);
            R r;
            if (resultSize < result.size()) {
                // get reused runner
                r = queryRunner(resultSize);
            } else {
                // create new runner
                result.add(r = newQueryRunner());
            }
            r.first = first;
            r.last = last;
            if (!collectSearchRows(r)) {
                r.clear();
                return false;
            }
            resultSize++;
            return true;
        }

        @Override
        public void reset(boolean beforeQuery) {
            if (resultSize != 0 && !resetAfterFind()) {
                // find was not called, need to just clear runners
                for (int i = 0; i < resultSize; i++) {
                    queryRunner(i).clear();
                }
                resultSize = 0;
            }
        }

        @Override
        public final List<Future<Cursor>> find() {
            if (resultSize == 0) {
                return Collections.emptyList();
            }
            findCalled = true;
            startQueryRunners(resultSize);
            return resultSize == result.size() ? result : result.subList(0, resultSize);
        }
    }

    /**
     * Lazy query runner base for subqueries and views.
     */
    private abstract static class QueryRunnerBase extends LazyFuture<Cursor> {
        protected final ViewIndex viewIndex;
        protected SearchRow first;
        protected SearchRow last;
        private boolean isLazyResult;

        QueryRunnerBase(ViewIndex viewIndex) {
            this.viewIndex = viewIndex;
        }

        protected void clear() {
            first = last = null;
        }

        @Override
        public final boolean reset() {
            if (isLazyResult) {
                resetViewTopFutureCursorAfterQuery();
            }
            if (super.reset()) {
                return true;
            }
            // this query runner was never executed, need to clear manually
            clear();
            return false;
        }

        protected final ViewCursor newCursor(ResultInterface localResult) {
            isLazyResult = localResult.isLazy();
            ViewCursor cursor = new ViewCursor(viewIndex, localResult, first, last);
            clear();
            return cursor;
        }

        protected abstract void resetViewTopFutureCursorAfterQuery();
    }

    /**
     * View index lookup batch for a simple SELECT.
     */
    private final class ViewIndexLookupBatch extends ViewIndexLookupBatchBase<QueryRunner> {
        ViewIndexLookupBatch(ViewIndex viewIndex) {
            super(viewIndex);
        }

        @Override
        protected QueryRunner newQueryRunner() {
            return new QueryRunner(viewIndex);
        }

        @Override
        protected boolean collectSearchRows(QueryRunner r) {
            return top.collectSearchRows();
        }

        @Override
        public boolean isBatchFull() {
            return top.isBatchFull();
        }

        @Override
        protected void startQueryRunners(int resultSize) {
            // we do batched find only for top table filter and then lazily run
            // the ViewIndex query for each received top future cursor
            List<Future<Cursor>> topFutureCursors = top.find();
            if (topFutureCursors.size() != resultSize) {
                throw DbException
                        .throwInternalError("Unexpected result size: " +
                                topFutureCursors.size() + ", expected :" +
                                resultSize);
            }
            for (int i = 0; i < resultSize; i++) {
                QueryRunner r = queryRunner(i);
                r.topFutureCursor = topFutureCursors.get(i);
            }
        }
    }

    /**
     * Query runner for SELECT.
     */
    private final class QueryRunner extends QueryRunnerBase {
        Future<Cursor> topFutureCursor;

        QueryRunner(ViewIndex viewIndex) {
            super(viewIndex);
        }

        @Override
        protected void clear() {
            super.clear();
            topFutureCursor = null;
        }

        @Override
        protected Cursor run() throws Exception {
            if (topFutureCursor == null) {
                // if the top cursor is empty then the whole query will produce
                // empty result
                return EMPTY_CURSOR;
            }
            viewIndex.setupQueryParameters(viewIndex.getSession(), first, last, null);
            JoinBatch.this.viewTopFutureCursor = topFutureCursor;
            ResultInterface localResult;
            boolean lazy = false;
            try {
                localResult = viewIndex.getQuery().query(0);
                lazy = localResult.isLazy();
            } finally {
                if (!lazy) {
                    resetViewTopFutureCursorAfterQuery();
                }
            }
            return newCursor(localResult);
        }

        @Override
        protected void resetViewTopFutureCursorAfterQuery() {
            JoinBatch.this.viewTopFutureCursor = null;
        }
    }

    /**
     * View index lookup batch for UNION queries.
     */
    private static final class ViewIndexLookupBatchUnion
            extends ViewIndexLookupBatchBase<QueryRunnerUnion> {
        ArrayList<JoinFilter> filters;
        ArrayList<JoinBatch> joinBatches;
        private boolean onlyBatchedQueries = true;

        protected ViewIndexLookupBatchUnion(ViewIndex viewIndex) {
            super(viewIndex);
        }

        boolean initialize() {
            return collectJoinBatches(viewIndex.getQuery()) && joinBatches != null;
        }

        private boolean collectJoinBatches(Query query) {
            if (query.isUnion()) {
                SelectUnion union = (SelectUnion) query;
                return collectJoinBatches(union.getLeft()) &&
                        collectJoinBatches(union.getRight());
            }
            Select select = (Select) query;
            JoinBatch jb = select.getJoinBatch();
            if (jb == null) {
                onlyBatchedQueries = false;
            } else {
                if (jb.getLookupBatch(0) == null) {
                    // we are top sub-query
                    return false;
                }
                assert !jb.batchedSubQuery;
                jb.batchedSubQuery = true;
                if (joinBatches == null) {
                    joinBatches = New.arrayList();
                    filters = New.arrayList();
                }
                filters.add(jb.filters[0]);
                joinBatches.add(jb);
            }
            return true;
        }

        @Override
        public boolean isBatchFull() {
            // if at least one is full
            for (JoinFilter filter : filters) {
                if (filter.isBatchFull()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean collectSearchRows(QueryRunnerUnion r) {
            boolean collected = false;
            for (int i = 0; i < filters.size(); i++) {
                if (filters.get(i).collectSearchRows()) {
                    collected = true;
                } else {
                    r.topFutureCursors[i] = EMPTY_FUTURE_CURSOR;
                }
            }
            return collected || !onlyBatchedQueries;
        }

        @Override
        protected QueryRunnerUnion newQueryRunner() {
            return new QueryRunnerUnion(this);
        }

        @Override
        protected void startQueryRunners(int resultSize) {
            for (int f = 0; f < filters.size(); f++) {
                List<Future<Cursor>> topFutureCursors = filters.get(f).find();
                int r = 0, c = 0;
                for (; r < resultSize; r++) {
                    Future<Cursor>[] cs = queryRunner(r).topFutureCursors;
                    if (cs[f] == null) {
                        cs[f] = topFutureCursors.get(c++);
                    }
                }
                assert r == resultSize;
                assert c == topFutureCursors.size();
            }
        }
    }

    /**
     * Query runner for UNION.
     */
    private static class QueryRunnerUnion extends QueryRunnerBase {
        Future<Cursor>[] topFutureCursors;
        private ViewIndexLookupBatchUnion batchUnion;

        @SuppressWarnings("unchecked")
        QueryRunnerUnion(ViewIndexLookupBatchUnion batchUnion) {
            super(batchUnion.viewIndex);
            this.batchUnion = batchUnion;
            topFutureCursors = new Future[batchUnion.filters.size()];
        }

        @Override
        protected void clear() {
            super.clear();
            for (int i = 0; i < topFutureCursors.length; i++) {
                topFutureCursors[i] = null;
            }
        }

        @Override
        protected Cursor run() throws Exception {
            viewIndex.setupQueryParameters(viewIndex.getSession(), first, last, null);
            ArrayList<JoinBatch> joinBatches = batchUnion.joinBatches;
            for (int i = 0, size = joinBatches.size(); i < size; i++) {
                assert topFutureCursors[i] != null;
                joinBatches.get(i).viewTopFutureCursor = topFutureCursors[i];
            }
            ResultInterface localResult;
            boolean lazy = false;
            try {
                localResult = viewIndex.getQuery().query(0);
                lazy = localResult.isLazy();
            } finally {
                if (!lazy) {
                    resetViewTopFutureCursorAfterQuery();
                }
            }
            return newCursor(localResult);
        }

        @Override
        protected void resetViewTopFutureCursorAfterQuery() {
            ArrayList<JoinBatch> joinBatches = batchUnion.joinBatches;
            if (joinBatches == null) {
                return;
            }
            for (JoinBatch joinBatch : joinBatches) {
                joinBatch.viewTopFutureCursor = null;
            }
        }
    }
}

