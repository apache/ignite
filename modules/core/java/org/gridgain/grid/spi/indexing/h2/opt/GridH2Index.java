/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import edu.stanford.ppl.concurrent.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.h2.engine.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;
import org.h2.table.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Base class for snapshotable tree indexes.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class GridH2Index extends BaseIndex implements Comparator<GridSearchRowPointer> {
    /** */
    private static final ThreadLocal<GridIndexingQueryFilter<?, ?>[]> filters =
        new ThreadLocal<>();

    /** */
    protected final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree;

    /** */
    private ThreadLocal<ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row>> snapshot =
        new ThreadLocal<>();

    /** */
    protected final int keyCol;

    /** */
    protected final int valCol;

    /**
     * Constructor with index initialization.
     *
     * @param name Index name.
     * @param tbl Table.
     * @param unique If this index unique.
     * @param keyCol Primary key column index.
     * @param valCol Value column index.
     * @param memory Memory.
     * @param cols Index columns list.
     */
    @SuppressWarnings("unchecked")
    public GridH2Index(String name, GridH2Table tbl, boolean unique, int keyCol, int valCol,
        final GridUnsafeMemory memory, IndexColumn... cols) {
        if (!unique) {
            // For non unique index we add primary key at the end to avoid conflicts.
            cols = Arrays.copyOf(cols, cols.length + 1);

            cols[cols.length - 1] = tbl.indexColumn(keyCol, SortOrder.ASCENDING);
        }

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            unique ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false, false));

        this.keyCol = keyCol;
        this.valCol = valCol;

        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        tree = memory == null ? new SnapTreeMap<GridSearchRowPointer, GridH2Row>(this) {
            @Override protected void afterNodeUpdate_nl(Node<GridSearchRowPointer, GridH2Row> node, Object val) {
                if (val != null)
                    node.key = (GridSearchRowPointer)val;
            }

            @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                if (key instanceof ComparableRow)
                    return (Comparable<? super SearchRow>)key;

                return super.comparable(key);
            }
        } : new GridOffHeapSnapTreeMap<GridSearchRowPointer, GridH2Row>(desc, desc, memory, this) {
            @Override protected void afterNodeUpdate_nl(long node, GridH2Row val) {
                final long oldKey = keyPtr(node);

                if (val != null) {
                    key(node, val);

                    memory.finalizeLater(new Runnable() {
                        @Override public void run() {
                            desc.createPointer(oldKey).decrementRefCount();
                        }
                    });
                }
            }

            @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                if (key instanceof ComparableRow)
                    return (Comparable<? super SearchRow>)key;

                return super.comparable(key);
            }
        };
    }

    /**
     * Closes index and releases resources.
     */
    public void close() {
        if (tree instanceof Closeable)
            U.closeQuiet((Closeable)tree);
    }

    /**
     * Sets key filters for current thread.
     *
     * @param fs Filters.
     */
    public static void setFiltersForThread(GridIndexingQueryFilter<?, ?>[] fs) {
        filters.set(fs);
    }

    /**
     * Takes snapshot to be used in current thread. If argument is null it will be taken from current trees.
     *
     * @param s Map to be used as snapshot if not null.
     * @return Taken snapshot or given argument back.
     */
    @SuppressWarnings("unchecked")
    public ConcurrentNavigableMap<SearchRow, GridH2Row> takeSnapshot(@Nullable ConcurrentNavigableMap s) {
        assert snapshot.get() == null;

        if (s == null)
            s = tree instanceof SnapTreeMap ? ((SnapTreeMap)tree).clone() :
                ((GridOffHeapSnapTreeMap)tree).clone();

        snapshot.set(s);

        return s;
    }

    /**
     * Releases snapshot for current thread.
     */
    public void releaseSnapshot() {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> s = snapshot.get();

        snapshot.remove();

        if (s instanceof Closeable)
            U.closeQuiet((Closeable)s);
    }

    /**
     * @return Snapshot for current thread if there is one.
     */
    private ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead() {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> res = snapshot.get();

        if (res == null)
            res = tree;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        assert snapshot.get() == null;
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(@Nullable Session ses) {
        Iterator<GridH2Row> iter = doFind(null, false, null);

        long size = 0;

        while (iter.hasNext()) {
            iter.next();

            size++;
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return tree.size();
    }

    /** {@inheritDoc} */
    @Override public int compare(GridSearchRowPointer r1, GridSearchRowPointer r2) {
        // Second row here must be data row if first is a search row.
        return -compareRows(r2, r1);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB((indexType.isUnique() ? "Unique index '" : "Index '") + getName() + "' [");

        boolean first = true;

        for (IndexColumn col : getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a(col.getSQL());
        }

        sb.a(" ]");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks) {
        return getCostRangeIndex(masks, getRowCountApproximation());
    }

    /** {@inheritDoc} */
    @Override public boolean canFindNext() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, @Nullable SearchRow first, @Nullable SearchRow last) {
        return new GridH2Cursor(doFind(first, true, last));
    }

    /** {@inheritDoc} */
    @Override public Cursor findNext(Session ses, SearchRow higherThan, SearchRow last) {
        return new GridH2Cursor(doFind(higherThan, false, last));
    }

    /**
     * Finds row with key equal one in given search row.
     * WARNING!! Method call must be protected by {@link GridUnsafeMemory#begin()}
     * {@link GridUnsafeMemory#end(GridUnsafeMemory.Operation)} block.
     *
     * @param row Search row.
     * @return Row.
     */
    public GridH2AbstractKeyValueRow findOne(GridSearchRowPointer row) {
        return (GridH2AbstractKeyValueRow)tree.get(row);
    }

    /**
     * Returns sub-tree bounded by given values.
     *
     * @param first Lower bound.
     * @param includeFirst Whether lower bound should be inclusive.
     * @param last Upper bound always inclusive.
     * @return Iterator over rows in given range.
     */
    @SuppressWarnings("unchecked")
    private Iterator<GridH2Row> doFind(@Nullable SearchRow first, boolean includeFirst,
        @Nullable SearchRow last) {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t = treeForRead();

        includeFirst &= first != null;

        NavigableMap<GridSearchRowPointer, GridH2Row> range = subTree(t, comparable(first, includeFirst ? -1 : 1),
            comparable(last, 1));

        if (range == null)
            return new GridEmptyIterator<>();

        return filter(range.values().iterator());
    }

    /**
     * @param row Row.
     * @param bias Bias.
     * @return Comparable row.
     */
    private GridSearchRowPointer comparable(SearchRow row, int bias) {
        if (row == null)
            return null;

        if (bias == 0 && row instanceof GridH2Row)
            return (GridSearchRowPointer)row;

        return new ComparableRow(row, bias);
    }

    /**
     * Filters rows from expired ones and using predicate.
     *
     * @param iter Iterator over rows.
     * @return Filtered iterator.
     */
    private Iterator<GridH2Row> filter(Iterator<GridH2Row> iter) {
        return new FilteringIterator(iter, U.currentTimeMillis());
    }

    /**
     * Takes sup-map from given one.
     *
     * @param map Map.
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Sub-map.
     */
    @SuppressWarnings({"IfMayBeConditional", "TypeMayBeWeakened"})
    private NavigableMap<GridSearchRowPointer, GridH2Row> subTree(NavigableMap<GridSearchRowPointer, GridH2Row> map,
        @Nullable GridSearchRowPointer first, @Nullable GridSearchRowPointer last) {
        // We take exclusive bounds because it is possible that one search row will be equal to multiple key rows
        // in tree and we must return them all.
        if (first == null) {
            if (last == null)
                return map;
            else
                return map.headMap(last, false);
        }
        else {
            if (last == null)
                return map.tailMap(first, false);
            else {
                if (compare(first, last) > 0)
                    return null;

                return map.subMap(first, false, last, false);
            }
        }
    }

    /**
     * Gets iterator over all rows in this index.
     *
     * @return Rows iterator.
     */
    Iterator<GridH2Row> rows() {
        return doFind(null, false, null);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree = treeForRead();

        GridSearchRowPointer res = null;

        Iterator<GridH2Row> iter = filter(first ? tree.values().iterator() :
            tree.descendingMap().values().iterator());

        if (iter.hasNext()) {
            GridSearchRowPointer r = iter.next();

            if ((first && compare(r, res) < 0) || (!first && compare(r, res) > 0))
                res = r;
        }

        return new SingleRowCursor((Row)res);
    }

    /**
     * Put row if absent.
     *
     * @param row Row.
     * @param ifAbsent Put only if such a row does not exist.
     * @return Existing row or null.
     */
    public GridH2Row put(GridH2Row row, boolean ifAbsent) {
        return ifAbsent ? tree.putIfAbsent(row, row) : tree.put(row, row);
    }

    /**
     * Remove row from index.
     *
     * @param row Row.
     * @return Removed row.
     */
    public GridH2Row remove(SearchRow row) {
        return tree.remove(comparable(row, 0));
    }
    /**
     * Iterator which filters by expiration time and predicate.
     */
    private class FilteringIterator implements Iterator<GridH2Row> {
        /** */
        private final Iterator<GridH2Row> iter;

        /** */
        private final GridIndexingQueryFilter<?, ?>[] fs = filters.get();

        /** */
        private final long time;

        /** */
        private GridH2Row curr;

        /**
         * @param iter Iterator.
         * @param time Time for expired rows filtering.
         */
        private FilteringIterator(Iterator<GridH2Row> iter, long time) {
            this.iter = iter;
            this.time = time;

            moveNext();
        }

        /**
         * Get next row.
         */
        private void moveNext() {
            curr = null;

            while (iter.hasNext()) {
                GridH2Row row = iter.next();

                if (accept(row)) {
                    curr = row;

                    break;
                }
            }
        }

        /**
         * @param row Row.
         * @return If this row was accepted.
         */
        @SuppressWarnings("unchecked")
        private boolean accept(SearchRow row) {
            if (row instanceof GridH2AbstractKeyValueRow) {
                if (((GridH2AbstractKeyValueRow) row).expirationTime() <= time)
                    return false;
            }

            if (F.isEmpty(fs))
                return true;

            String spaceName = ((GridH2Table)getTable()).spaceName();

            Object key = row.getValue(keyCol).getObject();
            Object val = row.getValue(valCol).getObject();

            assert key != null;
            assert val != null;

            for (GridIndexingQueryFilter f : fs) {
                if (f != null && !f.apply(spaceName, key, val))
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return curr != null;
        }

        /** {@inheritDoc} */
        @Override public GridH2Row next() {
            if (!hasNext())
                throw new NoSuchElementException();

            GridH2Row res = curr;

            moveNext();

            return res;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Comparable row with bias. Will be used for queries to have correct bounds (in case of multicolumn index
     * and query on few first columns we will multiple equal entries in tree).
     */
    private class ComparableRow implements GridSearchRowPointer, Comparable<SearchRow> {
        /** */
        private final SearchRow row;

        /** */
        private final int bias;

        /**
         * @param row Row.
         * @param bias Bias.
         */
        private ComparableRow(SearchRow row, int bias) {
            this.row = row;
            this.bias = bias;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(SearchRow o) {
            int res = compareRows(o, row);

            if (res == 0)
                return bias;

            return -res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return row.getColumnCount();
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return row.getValue(idx);
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            row.setValue(idx, v);
        }

        /** {@inheritDoc} */
        @Override public void setKeyAndVersion(SearchRow old) {
            row.setKeyAndVersion(old);
        }

        /** {@inheritDoc} */
        @Override public int getVersion() {
            return row.getVersion();
        }

        /** {@inheritDoc} */
        @Override public void setKey(long key) {
            row.setKey(key);
        }

        /** {@inheritDoc} */
        @Override public long getKey() {
            return row.getKey();
        }

        /** {@inheritDoc} */
        @Override public int getMemory() {
            return row.getMemory();
        }

        /** {@inheritDoc} */
        @Override public long pointer() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void incrementRefCount() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void decrementRefCount() {
            throw new IllegalStateException();
        }
    }

    /**
     * Creates copy of this index.
     *
     * @param memory Memory.
     * @return New index instance.
     * @throws InterruptedException If copy operation was interrupted.
     */
    GridH2Index createCopy(GridUnsafeMemory memory) throws InterruptedException {
        IndexColumn[] cols = getIndexColumns();

        if (!getIndexType().isUnique())
            cols = Arrays.copyOf(cols, cols.length - 1);

        GridH2Index idx = new GridH2Index(getName(), (GridH2Table)getTable(), getIndexType().isUnique(), keyCol, valCol,
            memory, cols);

        Thread thread = Thread.currentThread();

        long i = 0;

        for (GridH2Row row : tree.values()) {
            // Check for interruptions every 1000 iterations.
            if (++i % 1000 == 0 && thread.isInterrupted())
                throw new InterruptedException();

            idx.tree.put(row, row);
        }

        return idx;
    }
}
