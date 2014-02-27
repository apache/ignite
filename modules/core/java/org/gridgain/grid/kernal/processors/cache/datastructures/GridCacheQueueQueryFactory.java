// @java.file.header

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
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.query.GridCacheQueryType.*;

/**
 * Query factory responsible for providing all queries utilized by queue service.
 *
 * @author @java.author
 * @version @java.version
 */
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
    private GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemsQry;

    /** Query to first queue item. */
    private GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>> firstItemQry;

    /** Query to last queue item. */
    private GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>> lastItemQry;

    /** Query to get all queue keys. */
    private GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>> rmvAllKeysQry;

    /** Query to check contains of given items. */
    private GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        boolean[], Boolean> containsQry;

    /** Query to get keys of given items. */
    private GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>> rmvItemsQry;

    /** Query to get queue items at specified positions. */
    private GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemsAtPosQry;

    /** Query to get position of queue item. */
    private GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Integer, Integer> posOfItemQry;

    /** Queries object. */
    private GridCacheQueries<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> qry;

    /**
     * Comparator by sequence id.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    @GridToStringExclude
    private final SequenceComparator<T> seqComp = new SequenceComparator<>();

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
        itemsQry = qry.createQuery(SQL, GridCacheQueueItemImpl.class.getName(), "qid=? " +
            "order by seq asc");

        // Query to get items at specified positions from given queue.
        // Uses optimized H2 array syntax to avoid big IN(..) clauses.
        itemsAtPosQry = qry.createQuery(SQL, GridCacheQueueItemImpl.class.getName(),
            "select * from " +
                "(select *, rownum as r from " +
                "(select * from GridCacheQueueItemImpl where qid=? " + "order by seq asc" + ')' +
                ") where r-1 in (select * from table(x int=?))");

        // Query to get positions of given items in a queue.
        // The reducer will be set later, during call time.
        posOfItemQry = qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
            "qid=? " + "order by seq asc");
    }

    /**
     * Initialize contains query.
     */
    private void initRemoveAllKeysQuery() {
        // Query to get all keys in a queue.
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
            GridBiTuple<Integer, GridException>> rmvAllKeysQry = qry.createReduceQuery(SQL,
            GridCacheQueueItemImpl.class.getName(), "qid=? " + "order by seq asc");

        this.rmvAllKeysQry = rmvAllKeysQry.localReducer(new RemoveAllKeysQueryLocalReducer());
    }

    /**
     * Initialize contains query.
     */
    private void initRemoveItemsQuery() {
        // Query to check contains of given items in a queue.
        // Uses optimized H2 array syntax to avoid big IN(..) clauses.
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
            GridBiTuple<Integer, GridException>> rmvItemsQry = qry.createReduceQuery(SQL,
            GridCacheQueueItemImpl.class.getName(), " qid=? and id in (select * from table(x int=?)) " +
            "order by seq asc");

        this.rmvItemsQry = rmvItemsQry.localReducer(new RemoveItemsQueryLocalReducer());
    }

    /**
     * Initialize contains query.
     */
    private void initContainsQuery() {
        // Query to check contains of given items in a queue.
        // Uses optimized H2 array syntax to avoid big IN(..) clauses.
        containsQry = qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
            " qid=? and id in (select * from table(x int=?)) " + "order by seq asc");
    }

    /**
     * Initialize queue item query.
     */
    private void initQueueItemQuery() {
        // Reducer for receiving only one record from partitioned cache from primary node.
        OneRecordReducer<T> rdcOneRecord = new OneRecordReducer<>();

        // Query to first item (regarding to order) from given queue.
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> firstItemQry =
            qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
                " qid=? " + "order by seq asc");

        // Query to last item (regarding to order) from given queue.
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> lastItemQry =
            qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
                " qid=? " + "order by seq desc");

        this.firstItemQry = firstItemQry.remoteReducer(rdcOneRecord);
        this.lastItemQry = lastItemQry.remoteReducer(rdcOneRecord);
    }

    /**
     * @return Cache query for requesting all queue items.
     */
    GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemsQuery() {
        return itemsQry;
    }

    /**
     * @param items Items.
     * @param retain Retain flag.
     * @param single Single flag.
     * @return Cache query for requesting all queue keys of collection of queue item.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
        GridBiTuple<Integer, GridException>> itemsKeysQuery(
        final Iterable<?> items,
        final boolean retain,
        final boolean single) {
        return rmvItemsQry.remoteReducer(new RemoveItemsQueryRemoteReducer<>(cctx, itemView, items, retain, single));
    }

    /**
     * @param size Size to remove.
     * @return Cache query for requesting all queue keys.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
        GridBiTuple<Integer, GridException>> removeAllKeysQuery(final Integer size) {
        return rmvAllKeysQry.remoteReducer(new RemoveAllKeysQueryRemoteReducer<>(cctx, itemView, size));
    }

    /**
     * @param items Items.
     * @return Cache query for checking contains queue item.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, boolean[], Boolean> containsQuery(
        final Object[] items) {
        return containsQry
            .remoteReducer(new ContainsQueryRemoteReducer<T>(items))
            .localReducer(new ContainsQueryLocalReducer(items.length));
    }

    /**
     * @return Cache query for requesting all queue items.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>
        firstItemQuery() {
        return firstItemQry.localReducer(new TerminalItemQueryLocalReducer<>(seqComp, true));
    }

    /**
     * @return Cache query for requesting all queue items.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> lastItemQuery() {
        return lastItemQry.localReducer(new TerminalItemQueryLocalReducer<>(seqComp, false));
    }

    /**
     * @return Cache query for requesting queue items at specified positions.
     */
    GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemsAtPositionsQuery() {
        return itemsAtPosQry;
    }

    /**
     * @param item Object to find position for.
     * @return Cache query for requesting item position.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Integer, Integer> itemPositionQuery(
        final Object item) {
        return posOfItemQry.remoteReducer(new PositionQueryRemoteReducer<T>(item));
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
    private static class TerminalItemQueryLocalReducer<T>
        extends R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> {
        /** */
        private Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp;

        /** */
        private boolean first;

        /**
         * @param cmp Comparator.
         * @param first First flag.
         */
        private TerminalItemQueryLocalReducer(
            Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp, boolean first) {
            this.cmp = cmp;
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
    private static class RemoveItemsQueryLocalReducer
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
    private static class RemoveAllKeysQueryLocalReducer
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
    private static class ContainsQueryLocalReducer extends R1<boolean[], Boolean> {
        /** */
        private final boolean[] arr;

        /**
         * @param size Reduce size.
         */
        private ContainsQueryLocalReducer(int size) {
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
    private static class ContainsQueryRemoteReducer<T> extends R1<Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>,boolean[]> {
        /** Items. */
        private final Object[] items;

        /** */
        private final boolean[] retVal;

        /**
         * @param items Items.
         */
        private ContainsQueryRemoteReducer(Object[] items) {
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
                        GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC,
                            REPEATABLE_READ);

                        try {
                            itemView.removeAll(keys);

                            tx.commit();
                        }
                        finally {
                            tx.close();
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
                    GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC,
                        REPEATABLE_READ);

                    try {
                        itemView.removeAll(keys);

                        tx.commit();
                    }
                    finally {
                        tx.close();
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
                GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC, REPEATABLE_READ);

                try {
                    itemView.removeAll(keys);

                    tx.commit();
                }
                finally {
                    tx.close();
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
    private static class OneRecordReducer<T>
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
