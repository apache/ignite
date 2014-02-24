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
import org.gridgain.grid.cache.datastructures.*;
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
 * Query factory responsible for providing all queries utilized by queue service. Note
 * that different sorting rules are supported by merely adding proper {@code order by}
 * clause to a query. This way additional sorting rules can be added by simply specifying
 * new {@code order by} clauses in {@link #queueOrder(GridCacheQueueType)} method.
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

    /** Queries to get all queue items. */
    private final Map<GridCacheQueueType, GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>
        itemsQrys = new EnumMap<>
        (GridCacheQueueType.class);

    /** Queries to first queue item. */
    private final Map<GridCacheQueueType, GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>>> firstItemQrys = new EnumMap<>(
                GridCacheQueueType.class);

    /** Queries to last queue item. */
    private final Map<GridCacheQueueType, GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>>> lastItemQrys = new EnumMap<>(
                GridCacheQueueType.class);

    /** Queries to get all queue keys. */
    private final Map<GridCacheQueueType, GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>> rmvAllKeysQrys =
        new EnumMap<>(GridCacheQueueType.class);

    /** Queries to check contains of given items. */
    private final Map<GridCacheQueueType, GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        boolean[], Boolean>> containsQrys = new EnumMap<>(GridCacheQueueType.class);

    /** Queries to get keys of given items. */
    private final Map<GridCacheQueueType, GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>> rmvItemsQrys =
        new EnumMap<>(GridCacheQueueType.class);

    /** Queries to get queue items at specified positions. */
    private final Map<GridCacheQueueType, GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>
        itemsAtPosQrys =
        new EnumMap<>
            (GridCacheQueueType.class);

    /** Queries to get position of queue item. */
    private final Map<GridCacheQueueType, GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
        Integer, Integer>> posOfItemQrys = new EnumMap<>(GridCacheQueueType.class);

    /** Queries object. */
    private GridCacheQueries<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> qry;

    /**
     * Comparator by sequence id.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    @GridToStringExclude
    private final SequenceComparator<T> seqComp = new SequenceComparator<>();

    /**
     * Comparator by priority.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    @GridToStringExclude
    private final PriorityComparator<T> priComp = new PriorityComparator<>();

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
     * This method provides {@code order by} SQL clauses for different queue types (or sorting rules).
     * Additional queue types can be supported by providing proper {@code order by} clauses here.
     *
     * @param type Queue type.
     * @return Order by sql clause.
     */
    private String queueOrder(GridCacheQueueType type) {
        switch (type) {
            case FIFO:
                return "order by seq asc";
            case LIFO:
                return "order by seq desc";
            case PRIORITY:
                return "order by priority desc, seq asc";

            default:
                throw new RuntimeException("Unknown queue type: " + type);
        }
    }

    /**
     * This method provides {@code order by} SQL clauses for different dequeue types (or sorting rules).
     * Additional queue types can be supported by providing proper {@code order by} clauses here.
     *
     * @param type Queue type.
     * @return Order by sql clause.
     */
    private String dequeueOrder(GridCacheQueueType type) {
        switch (type) {
            case FIFO:
                return "order by seq desc";
            case LIFO:
                return "order by seq asc";
            case PRIORITY:
                return "order by priority asc, seq desc";

            default:
                throw new RuntimeException("Unknown queue type: " + type);
        }
    }

    /**
     * Initialize item queries.
     */
    private void initItemsQueries() {
        // Pre-create all queries for all queue types.
        for (GridCacheQueueType type : GridCacheQueueType.values()) {
            // Query to get ordered items from given queue.
            itemsQrys.put(type, qry.createQuery(SQL, GridCacheQueueItemImpl.class.getName(), "qid=? " +
                queueOrder(type)));

            // Query to get items at specified positions from given queue.
            // Uses optimized H2 array syntax to avoid big IN(..) clauses.
            itemsAtPosQrys.put(type, qry.createQuery(SQL, GridCacheQueueItemImpl.class.getName(),
                "select * from " +
                    "(select *, rownum as r from " +
                    "(select * from GridCacheQueueItemImpl where qid=? " + queueOrder(type) + ')' +
                    ") where r-1 in (select * from table(x int=?))"));

            // Query to get positions of given items in a queue.
            // The reducer will be set later, during call time.
            GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Integer, Integer> posOfItemQry =
                qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(), "qid=?" + queueOrder(type));

            posOfItemQrys.put(type, posOfItemQry);
        }
    }

    /**
     * Initialize contains query.
     */
    private void initRemoveAllKeysQuery() {
        // Pre-create contains query for all queue types.
        for (GridCacheQueueType type : GridCacheQueueType.values()) {
            // Query to get all keys in a queue.
            GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
                GridBiTuple<Integer, GridException>> rmvAllKeysQry = qry.createReduceQuery(SQL,
                GridCacheQueueItemImpl.class.getName(), "qid=? " + queueOrder(type));

            rmvAllKeysQry = rmvAllKeysQry.localReducer(new RemoveAllKeysQueryLocalReducer());

            rmvAllKeysQrys.put(type, rmvAllKeysQry);
        }
    }

    /**
     * Initialize contains query.
     */
    private void initRemoveItemsQuery() {
        // Pre-create contains query for all queue types.
        for (GridCacheQueueType type : GridCacheQueueType.values()) {
            // Query to check contains of given items in a queue.
            // Uses optimized H2 array syntax to avoid big IN(..) clauses.
            GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
                GridBiTuple<Integer, GridException>> rmvItemsQry = qry.createReduceQuery(SQL,
                GridCacheQueueItemImpl.class.getName(), " qid=? and id in (select * from table(x int=?)) " +
                queueOrder(type));

            rmvItemsQry = rmvItemsQry.localReducer(new RemoveItemsQueryLocalReducer());

            rmvItemsQrys.put(type, rmvItemsQry);
        }
    }

    /**
     * Initialize contains query.
     */
    private void initContainsQuery() {
        // Pre-create contains query for all queue types.
        for (GridCacheQueueType type : GridCacheQueueType.values()) {
            // Query to check contains of given items in a queue.
            // Uses optimized H2 array syntax to avoid big IN(..) clauses.
            GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, boolean[],
                Boolean> containsQry = qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
                " qid=? and id in (select * from table(x int=?)) " + queueOrder(type));

            containsQrys.put(type, containsQry);
        }
    }

    /**
     * Initialize queue item query.
     */
    private void initQueueItemQuery() {
        // Reducer for receiving only one record from partitioned cache from primary node.
        OneRecordReducer<T> rdcOneRecord = new OneRecordReducer<>();

        // Pre-create contains query for all queue types.
        for (GridCacheQueueType type : GridCacheQueueType.values()) {
            // Query to first item (regarding to order) from given queue.
            GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> firstItemQry =
                qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
                    " qid=? " + queueOrder(type));

            // Query to last item (regarding to order) from given queue.
            GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> lastItemQry =
                qry.createReduceQuery(SQL, GridCacheQueueItemImpl.class.getName(),
                    " qid=? " + dequeueOrder(type));

            firstItemQry = firstItemQry.remoteReducer(rdcOneRecord);
            lastItemQry = lastItemQry.remoteReducer(rdcOneRecord);

            firstItemQrys.put(type, firstItemQry);
            lastItemQrys.put(type, lastItemQry);
        }
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting all queue items.
     */
    GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemsQuery(GridCacheQueueType type) {
        return itemsQrys.get(type);
    }

    /**
     * @param type Queue type.
     * @param items Items.
     * @param retain Retain flag.
     * @param single Single flag.
     * @return Cache query for requesting all queue keys of collection of queue item.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
        GridBiTuple<Integer, GridException>> itemsKeysQuery(
        GridCacheQueueType type,
        final Iterable<?> items,
        final boolean retain,
        final boolean single) {
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
            GridBiTuple<Integer, GridException>> rmvItemsQry = rmvItemsQrys.get(type);

        return rmvItemsQry.remoteReducer(new RemoveItemsQueryRemoteReducer<>(cctx, itemView, items, retain, single));
    }

    /**
     * @param type Queue type.
     * @param size Size to remove.
     * @return Cache query for requesting all queue keys.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
        GridBiTuple<Integer, GridException>> removeAllKeysQuery(GridCacheQueueType type, final Integer size) {
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
            GridBiTuple<Integer, GridException>>rmvAllKeysQry = rmvAllKeysQrys.get(type);

        return rmvAllKeysQry.remoteReducer(new RemoveAllKeysQueryRemoteReducer<>(cctx, itemView, size));
    }

    /**
     * @param type Queue type.
     * @param items Items.
     * @return Cache query for checking contains queue item.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, boolean[], Boolean>
        containsQuery(GridCacheQueueType type, final Object[] items) {
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, boolean[], Boolean>
            containsQry = containsQrys.get(type);

        return containsQry
            .remoteReducer(new ContainsQueryRemoteReducer<T>(items))
            .localReducer(new ContainsQueryLocalReducer(items.length));
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting all queue items.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>
        firstItemQuery(final GridCacheQueueType type) {
        GridCacheReduceQuery<
            GridCacheQueueItemKey,
            GridCacheQueueItemImpl<T>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>
            firstItemQry = firstItemQrys.get(type);

        Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp =
            type == GridCacheQueueType.PRIORITY ? priComp : seqComp;

        return firstItemQry.localReducer(new TerminalItemQueryLocalReducer<>(type, cmp, true));
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting all queue items.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> lastItemQuery(
        final GridCacheQueueType type) {
        GridCacheReduceQuery<
            GridCacheQueueItemKey,
            GridCacheQueueItemImpl<T>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> lastItemQry = lastItemQrys.get(type);

        Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp =
            type == GridCacheQueueType.PRIORITY ? priComp : seqComp;

        return lastItemQry.localReducer(new TerminalItemQueryLocalReducer<>(type, cmp, false));
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting queue items at specified positions.
     */
    GridCacheQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemsAtPositionsQuery(GridCacheQueueType type) {
        return itemsAtPosQrys.get(type);
    }

    /**
     * @param type Queue type.
     * @param item Object to find position for.
     * @return Cache query for requesting item position.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Integer, Integer> itemPositionQuery(
        GridCacheQueueType type, final Object item) {
        GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Integer, Integer> posOfItemQry =
            posOfItemQrys.get(type);

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
        private GridCacheQueueType type;

        /** */
        private Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp;

        /** */
        private boolean first;

        /**
         * @param type Queue type.
         * @param cmp Comparator.
         * @param first First flag.
         */
        private TerminalItemQueryLocalReducer(GridCacheQueueType type,
            Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> cmp,
            boolean first) {
            this.type = type;
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

            switch (type) {
                case FIFO:
                    return first ? Collections.min(items, cmp) : Collections.max(items, cmp);
                case LIFO:
                    return first ? Collections.max(items, cmp) : Collections.min(items, cmp);
                case PRIORITY:
                    return first ? Collections.min(items, cmp) : Collections.max(items, cmp);
                default:
                    assert false : "Unknown queue type: " + type;

                    return null;
            }
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

    private static class SequenceComparator<T>
        implements Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>, Serializable {
        @Override public int compare(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item1,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item2) {
            return (int)(item1.getValue().sequence() - item2.getValue().sequence());
        }

        @Override public String toString() {
            return "AAA";
        }
    }

    private static class PriorityComparator<T>
        implements Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>, Serializable {
        @Override public int compare(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item1,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item2) {
            int retVal = item1.getValue().priority() - item2.getValue().priority();

            // If items have equals priority, item with minimum sequence has more priority,
            if (retVal == 0)
                retVal = (int)(item1.getValue().sequence() - item2.getValue().sequence());

            return retVal;
        }

        @Override public String toString() {
            return "BBB";
        }
    }
}
