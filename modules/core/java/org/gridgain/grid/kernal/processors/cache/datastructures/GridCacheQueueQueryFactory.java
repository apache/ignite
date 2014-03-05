/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Query factory responsible for providing all queries utilized by queue service.
 */
@SuppressWarnings("PackageVisibleInnerClass")
class GridCacheQueueQueryFactory<T> implements Externalizable {
    /** Deserialization stash. */
    private static final ThreadLocal<GridTuple<GridCacheContext>> stash =
        new ThreadLocal<GridTuple<GridCacheContext>>() {
            @Override protected GridTuple<GridCacheContext> initialValue() {
                return F.t1();
            }
        };

    /** Cache context. */
    private GridCacheContext cctx;

    /** Query to get all queue items. */
    private GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> itemsQry;

    /** Query to first queue item. */
    private GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> firstItemQry;

    /** Query to get all queue keys. */
    private GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> rmvAllKeysQry;

    /** Query to check contains of given items. */
    private GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> containsQry;

    /** Query to get keys of given items. */
    private GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> rmvItemsQry;

    /** Query to get queue items at specified positions. */
    private GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> itemsAtPosQry;

    /** Queries object. */
    private GridCacheQueries<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> qry;

    /**
     * Comparator by sequence id.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    @GridToStringExclude
    private static final SequenceComparator<?> SEQ_COMP = new SequenceComparator<>();

    /** Queue items view.*/
    private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemView;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     */
    GridCacheQueueQueryFactory(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        itemView = cctx.cache().<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>
            projection(GridCacheQueueItemKey.class, GridCacheQueueItemImpl.class).flagsOn(CLONE);

        // Do not get proxy for queries.
        qry = ((GridCacheContext<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>)cctx).cache().queries();

        initItemsQueries();

        initContainsQuery();

        initRemoveItemsQuery();

        initRemoveAllKeysQuery();

        initQueueItemQuery();

        this.cctx = cctx;
    }

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheQueueQueryFactory() {
        // No-op.
    }

    /**
     * Initialize item queries.
     */
    private void initItemsQueries() {
        // Query to get ordered items from given queue.
        itemsQry = qry.createSqlQuery(GridCacheQueueItemImpl.class, "qid=? order by seq asc");

        // Query to get items at specified positions from given queue.
        // Uses optimized H2 array syntax to avoid big IN(..) clauses.
        itemsAtPosQry = qry.createSqlQuery(GridCacheQueueItemImpl.class,
            "select * from " +
                "(select *, rownum as r from " +
                "(select * from GridCacheQueueItemImpl where qid=? " + "order by seq asc" + ')' +
                ") where r-1 in (select * from table(x int=?))");
    }

    /**
     * Initialize contains query.
     */
    private void initRemoveAllKeysQuery() {
        // Query to get all keys in a queue.
        rmvAllKeysQry = qry.createSqlQuery(GridCacheQueueItemImpl.class, "qid=? " + "order by seq asc");
    }

    /**
     * Initialize contains query.
     */
    private void initRemoveItemsQuery() {
        // Query to check contains of given items in a queue.
        // Uses optimized H2 array syntax to avoid big IN(..) clauses.
        rmvItemsQry = qry.createSqlQuery(GridCacheQueueItemImpl.class,
            "qid=? and id in (select * from table(x int=?)) " + "order by seq asc");
    }

    /**
     * Initialize contains query.
     */
    private void initContainsQuery() {
        // Query to check contains of given items in a queue.
        // Uses optimized H2 array syntax to avoid big IN(..) clauses.
        containsQry = qry.createSqlQuery(GridCacheQueueItemImpl.class,
            " qid=? and id in (select * from table(x int=?)) " + "order by seq asc");
    }

    /**
     * Initialize queue item query.
     */
    private void initQueueItemQuery() {
        // Query to first item (regarding to order) from given queue.
        firstItemQry = qry.createSqlQuery(GridCacheQueueItemImpl.class,
                " qid=? " + "order by seq asc");
    }

    /**
     * @return Cache query for requesting all queue items.
     */
    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> itemsQuery() {
        return itemsQry;
    }

    /**
     * @return Cache query for requesting all queue keys of collection of queue item.
     */
    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> itemsKeysQuery() {
        return rmvItemsQry;
    }

    /**
     * @param items Items.
     * @param retain Retain.
     * @param single Single.
     * @return Reducer.
     */
    GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, GridBiTuple<Integer, GridException>>
        itemsKeysReducer(Iterable<?> items, boolean retain, boolean single) {
        return new RemoveItemsQueryRemoteReducer<>(cctx, itemView, items, retain, single);
    }

    /**
     * @return Cache query for requesting all queue keys.
     */
    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> removeAllKeysQuery() {
        return rmvAllKeysQry;
    }

    /**
     * @param size Size.
     * @return Reducer.
     */
    GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, GridBiTuple<Integer, GridException>>
        removeAllKeysReducer(int size) {
        return new RemoveAllKeysQueryRemoteReducer<>(cctx, itemView, size);
    }

    /**
     * @return Cache query for checking contains queue item.
     */
    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> containsQuery() {
        return containsQry;
    }

    /**
     * @return Cache query for requesting all queue items.
     */
    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> firstItemQuery() {
        return firstItemQry;
    }

    /**
     * @return Cache query for requesting queue items at specified positions.
     */
    GridCacheQuery<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> itemsAtPositionsQuery() {
        return itemsAtPosQry;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stash.get().set((GridCacheContext)in.readObject());
    }

    /**
     * Reconstructs object on demarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of demarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            GridTuple<GridCacheContext> t = stash.get();

            // Have to use direct class cast.
            return ((GridCacheEnterpriseDataStructuresManager)t.get().dataStructures()).queueQueryFactory();
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueQueryFactory.class, this);
    }

    /**
     *
     */
    static class TerminalItemQueryLocalReducer<T>
        extends R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> {
        /** */
        @SuppressWarnings("OverlyStrongTypeCast")
        private final Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp =
            (SequenceComparator<T>)SEQ_COMP;

        /** */
        private boolean first;

        /**
         * @param first First flag.
         */
        TerminalItemQueryLocalReducer(boolean first) {
            this.first = first;
        }

        /** */
        private final Collection<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> items =
            new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> e) {
            if (e != null)
                items.add(e);

            return true;
        }

        /** {@inheritDoc} */
        @Override public Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> reduce() {
            if (items.isEmpty())
                return null;

            return first ? Collections.min(items, cmp) : Collections.max(items, cmp);
        }
    }

    /**
     *
     */
    static class RemoveItemsQueryLocalReducer
        extends R1<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>> {
        /** */
        private final GridBiTuple<Integer, GridException> retVal = new T2<>(0, null);

        /** {@inheritDoc} */
        @Override public boolean collect(GridBiTuple<Integer, GridException> tup) {
            synchronized (this) {
                if (tup != null)
                    retVal.set(retVal.get1() + tup.get1(), tup.get2() != null ? tup.get2() :
                        retVal.get2());
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public GridBiTuple<Integer, GridException> reduce() {
            return retVal;
        }
    }

    /**
     *
     */
    static class RemoveAllKeysQueryLocalReducer
        extends R1<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>> {
        /** */
        private final GridBiTuple<Integer, GridException> retVal = new T2<>(0, null);

        @Override public boolean collect(GridBiTuple<Integer, GridException> tup) {
            synchronized (this) {
                if (tup != null)
                    retVal.set(retVal.get1() + tup.get1(), tup.get2() != null ? tup.get2() :
                        retVal.get2());
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public GridBiTuple<Integer, GridException> reduce() {
            return retVal;
        }
    }

    /**
     *
     */
    private static class PositionQueryRemoteReducer<T> extends
        R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, Integer> {
        /** */
        private Object item;

        /** */
        private int rownum = -1;

        /** */
        private boolean found;

        /**
         * @param item Item.
         */
        private PositionQueryRemoteReducer(Object item) {
            this.item = item;
        }

        /** {@inheritDoc} */
        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey,
            GridCacheQueueItemImpl<T>> e) {
            rownum++;

            return !(found = e.getValue().userObject().equals(item));
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return found ? rownum : -1;
        }
    }

    /**
     *
     */
    static class ContainsQueryLocalReducer extends R1<boolean[], Boolean> {
        /** */
        private final boolean[] arr;

        /**
         * @param size Reduce size.
         */
        ContainsQueryLocalReducer(int size) {
            arr = new boolean[size];
        }

        /** {@inheritDoc} */
        @Override public boolean collect(boolean[] e) {
            assert arr.length == e.length;

            synchronized (this) {
                for (int i = 0; i < e.length; i++)
                    arr[i] |= e[i];
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public Boolean reduce() {
            boolean retVal = true;

            for (boolean f : arr)
                retVal &= f;

            return retVal;
        }
    }

    /**
     *
     */
    static class ContainsQueryRemoteReducer<T> extends R1<Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, boolean[]> {
        /** Items. */
        private final Object[] items;

        /** */
        private final boolean[] retVal;

        /**
         * @param items Items.
         */
        ContainsQueryRemoteReducer(Object[] items) {
            this.items = items;

            retVal = new boolean[items.length];
        }

        /** {@inheritDoc} */
        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey,
            GridCacheQueueItemImpl<T>> e) {
            boolean found = true;

            for (int i = 0; i < retVal.length; i++) {
                if (!retVal[i])
                    retVal[i] = e.getValue().userObject().equals(items[i]);

                found &= retVal[i];
            }

            return !found;
        }

        /** {@inheritDoc} */
        @Override public boolean[] reduce() {
            return retVal;
        }
    }

    /**
     *
     */
    private static class RemoveAllKeysQueryRemoteReducer<T>
        extends R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, GridBiTuple<Integer, GridException>> {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private GridCacheContext cctx;

        /** */
        private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemView;

        /** */
        private int size;

        /** */
        private GridBiTuple<Integer, GridException> retVal = new T2<>(0, null);

        /** */
        private final Collection<GridCacheQueueItemKey> keys = new ArrayList<>(size);

        /**
         * @param cctx Cache context.
         * @param itemView Items view.
         * @param size Size to remove.
         */
        private RemoveAllKeysQueryRemoteReducer(
            GridCacheContext cctx,
            GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemView,
            int size
        ) {
            this.cctx = cctx;
            this.itemView = itemView;
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey,
            GridCacheQueueItemImpl<T>> entry) {
            try {
                // Check that entry wasn't already removed.
                if (itemView.get(entry.getKey()) != null) {
                    keys.add(entry.getKey());

                    if (size > 0 && keys.size() == size) {

                        try (GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC, REPEATABLE_READ)) {
                            itemView.removeAll(keys);

                            tx.commit();
                        }

                        retVal.set1(retVal.get1() + size);

                        keys.clear();
                    }
                }
                else
                    itemView.removex(entry.getKey());
            }
            catch (GridException e) {
                U.error(log, "Failed to remove items: " + keys, e);

                retVal.set2(e);

                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public GridBiTuple<Integer, GridException> reduce() {
            try {
                if (!keys.isEmpty()) {

                    try (GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC, REPEATABLE_READ)) {
                        itemView.removeAll(keys);

                        tx.commit();
                    }

                    retVal.set1(retVal.get1() + keys.size());

                    keys.clear();
                }
            }
            catch (GridException e) {
                U.error(log, "Failed to remove items: " + keys, e);

                retVal.set2(e);
            }

            return retVal;
        }
    }

    /**
     * Remove items query remote reducer.
     */
    private static class RemoveItemsQueryRemoteReducer<T> extends R1<Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, GridBiTuple<Integer, GridException>> {
        /** */
        private GridCacheContext cctx;

        /** */
        private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemView;

        /** */
        private Iterable<?> items;

        /** */
        private boolean retain;

        /** */
        private boolean single;

        /** */
        private Collection<GridCacheQueueItemKey> keys = new HashSet<>();

        /** */
        private GridBiTuple<Integer, GridException> retVal = new T2<>(0, null);

        /**
         * @param cctx Cache context.
         * @param itemView Items view.
         * @param items Items.
         * @param retain Retain flag.
         * @param single Single flag.
         */
        private RemoveItemsQueryRemoteReducer(
            GridCacheContext cctx,
            GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemView,
            Iterable<?> items,
            boolean retain,
            boolean single
        ) {
            this.cctx = cctx;
            this.itemView = itemView;
            this.items = items;
            this.retain = retain;
            this.single = single;
        }

        /** {@inheritDoc} */
        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey,
            GridCacheQueueItemImpl<T>> entry) {

            // Check that item were already removed;
            try {
                if (itemView.get(entry.getKey()) == null) {
                    itemView.removex(entry.getKey());

                    return false;
                }
            }
            catch (GridException e) {
                retVal.set2(e);
            }

            boolean found = false;

            for (Object item : items) {
                assert entry.getValue() != null;
                assert entry.getValue().userObject() != null;

                if (entry.getValue().userObject().equals(item)) {
                    found = true;

                    break;
                }
            }

            if (retain && !found || !retain && found) {
                keys.add(entry.getKey());

                // In case if we execute command removeItem.
                if (single && !retain)
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public GridBiTuple<Integer, GridException> reduce() {
            //If exception already happened.
            if (retVal.get2() != null)
                return retVal;

            try {

                try (GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC, REPEATABLE_READ)) {
                    itemView.removeAll(keys);

                    tx.commit();
                }

                retVal.set1(keys.size());
            }
            catch (GridException e) {
                retVal.set2(e);
            }

            return retVal;
        }
    }

    /**
     * One record reducer.
     */
    static class OneRecordReducer<T>
        extends GridReducer<Map.Entry<GridCacheQueueItemKey,GridCacheQueueItemImpl<T>>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> {
        /** */
        private Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> entry;

        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> e) {
            entry = new GridCacheQueryResponseEntry<>(e.getKey(), e.getValue());

            return false;
        }

        /** {@inheritDoc} */
        @Override public Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> reduce() {
            return entry;
        }
    }

    /**
     *
     */
    private static class SequenceComparator<T>
        implements Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>, Serializable {
        @Override public int compare(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item1,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item2) {
            return (int)(item1.getValue().sequence() - item2.getValue().sequence());
        }
    }
}
