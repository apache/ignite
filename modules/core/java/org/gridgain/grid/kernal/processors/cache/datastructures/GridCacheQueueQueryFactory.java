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

    /** Logger. */
    private GridLogger log;

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
    @GridToStringExclude
    private final Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> seqComp =
        new Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>() {
            @Override public int compare(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item1,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item2) {
                return (int)(item1.getValue().sequence() - item2.getValue().sequence());
            }
        };

    /**
     * Comparator by priority.
     */
    @GridToStringExclude
    private final Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> priComp =
        new Comparator<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>() {
            @Override public int compare(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item1,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> item2) {
                int retVal = item1.getValue().priority() - item2.getValue().priority();

                // If items have equals priority, item with minimum sequence has more priority,
                if (retVal == 0)
                    retVal = (int)(item1.getValue().sequence() - item2.getValue().sequence());

                return retVal;
            }
        };

    /** Queue items view.*/
    private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> itemView;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     */
    GridCacheQueueQueryFactory(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        log = cctx.logger(GridCacheQueueImpl.class);

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

            posOfItemQry.remoteReducer(
                new C1<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    Integer>>() {
                    @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                        Integer> apply(final Object[] args) {
                        assert args != null && args.length == 1 : "Invalid query reducer argument: "
                            + Arrays.toString(args);

                        return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, Integer>() {
                            private Object item = args[0];
                            private int rownum = -1;
                            private boolean found;

                            @Override public boolean collect(Map.Entry<GridCacheQueueItemKey,
                                GridCacheQueueItemImpl<T>> e) {
                                rownum++;

                                return !(found = e.getValue().userObject().equals(item));
                            }

                            @Override public Integer apply() {
                                return found ? rownum : -1;
                            }
                        };
                    }
                });
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

            rmvAllKeysQrys.put(type, rmvAllKeysQry);

            rmvAllKeysQry.remoteReducer(
                new C1<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    GridBiTuple<Integer, GridException>>>() {
                    @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                        GridBiTuple<Integer, GridException>> apply(final Object[] args) {
                        assert args != null && args.length == 1 : "Invalid query reducer argument: " +
                            Arrays.toString(args);

                        return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                            GridBiTuple<Integer, GridException>>() {
                            private Integer size = (Integer)args[0];

                            private GridBiTuple<Integer, GridException> retVal = new T2<>(0, null);

                            private final Collection<GridCacheQueueItemKey> keys = new ArrayList<>(
                                size);

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
                                                tx.end();
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

                            @Override public GridBiTuple<Integer, GridException> apply() {
                                try {
                                    if (!keys.isEmpty()) {
                                        GridCacheTx tx = CU.txStartInternal(cctx, itemView, PESSIMISTIC,
                                            REPEATABLE_READ);

                                        try {
                                            itemView.removeAll(keys);

                                            tx.commit();
                                        }
                                        finally {
                                            tx.end();
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
                        };
                    }
                });

            rmvAllKeysQry.localReducer(
                new C1<Object[], GridReducer<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>>() {
                    @Override public GridReducer<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>
                    apply(Object[] args) {
                        return new R1<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>() {
                            private final GridBiTuple<Integer, GridException> retVal = new T2<>(0,
                                null);

                            @Override public boolean collect(GridBiTuple<Integer, GridException> tup) {
                                synchronized (this) {
                                    if (tup != null)
                                        retVal.set(retVal.get1() + tup.get1(), tup.get2() != null ? tup.get2() :
                                            retVal.get2());
                                }

                                return true;
                            }

                            @Override public GridBiTuple<Integer, GridException> apply() {
                                return retVal;
                            }
                        };
                    }
                });
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

            rmvItemsQrys.put(type, rmvItemsQry);

            rmvItemsQry.remoteReducer(
                new C1<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    GridBiTuple<Integer, GridException>>>() {
                    @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                        GridBiTuple<Integer, GridException>> apply(final Object[] args) {
                        assert args != null && args.length == 3 : "Invalid query reducer argument: " +
                            Arrays.toString(args);

                        return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                            GridBiTuple<Integer, GridException>>() {
                            private Iterable<?> items = (Iterable<?>)args[0];

                            private boolean retain = (Boolean)args[1];

                            private boolean single = (Boolean)args[2];

                            private Collection<GridCacheQueueItemKey> keys = new HashSet<>();

                            private GridBiTuple<Integer, GridException> retVal = new T2<>(0, null);

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

                            @Override public GridBiTuple<Integer, GridException> apply() {
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
                                        tx.end();
                                    }

                                    retVal.set1(keys.size());
                                }
                                catch (GridException e) {
                                    retVal.set2(e);
                                }

                                return retVal;
                            }
                        };
                    }
                });

            rmvItemsQry.localReducer(
                new C1<Object[], GridReducer<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>>() {
                    @Override public GridReducer<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>
                    apply(Object[] args) {
                        return new R1<GridBiTuple<Integer, GridException>, GridBiTuple<Integer, GridException>>() {
                            private final GridBiTuple<Integer, GridException> retVal = new T2<>(0,
                                null);

                            @Override public boolean collect(GridBiTuple<Integer, GridException> tup) {
                                synchronized (this) {
                                    if (tup != null)
                                        retVal.set(retVal.get1() + tup.get1(), tup.get2() != null ? tup.get2() :
                                            retVal.get2());
                                }

                                return true;
                            }

                            @Override public GridBiTuple<Integer, GridException> apply() {
                                return retVal;
                            }
                        };
                    }
                });
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

            containsQry.remoteReducer(
                new C1<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    boolean[]>>() {
                    @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                        boolean[]> apply(final Object[] args) {
                        assert args != null && args.length == 2 : "Invalid query reducer argument: " +
                            Arrays.toString(args);

                        return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>, boolean[]>() {
                            private Object[] items = (Object[])args[1];

                            private boolean[] retVal = new boolean[items.length];

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

                            @Override public boolean[] apply() {
                                return retVal;
                            }
                        };
                    }
                });

            containsQry.localReducer(
                new C1<Object[], GridReducer<boolean[], Boolean>>() {
                    @Override public GridReducer<boolean[], Boolean> apply(final Object[] args) {
                        assert args != null && args.length == 2 : "Invalid query reducer argument: " +
                            Arrays.toString(args);

                        return new R1<boolean[], Boolean>() {
                            // Argument must be array.
                            private final boolean[] arr = new boolean[((Object[])args[1]).length];

                            private final Object mux = new Object();

                            @Override public boolean collect(boolean[] e) {
                                assert arr.length == e.length;

                                synchronized (mux) {
                                    for (int i = 0; i < e.length; i++)
                                        arr[i] |= e[i];
                                }
                                return true;
                            }

                            @Override public Boolean apply() {
                                boolean retVal = true;

                                for (boolean f : arr)
                                    retVal &= f;

                                return retVal;
                            }
                        };
                    }
                });
        }
    }

    /**
     * Initialize queue item query.
     */
    private void initQueueItemQuery() {
        /** Reducer for receiving only one record from partitioned cache from primary node. */
        GridClosure<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
            Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>> rdcOneRecord = new C1<Object[],
            GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>>() {
            @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> apply(Object[] args) {

                return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>() {
                    private Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> entry;

                    @Override public boolean collect(Map.Entry<GridCacheQueueItemKey,
                        GridCacheQueueItemImpl<T>> e) {
                        entry = new GridCacheQueryResponseEntry<>
                            (e.getKey(), e.getValue());

                        return false;
                    }

                    @Override public Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> apply() {
                        return entry;
                    }
                };
            }
        };

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

            firstItemQrys.put(type, firstItemQry);
            lastItemQrys.put(type, lastItemQry);

            firstItemQry.remoteReducer(rdcOneRecord);

            firstItemQry.localReducer(new C1<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey,
                GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>>() {
                @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> apply(final Object[] args) {
                    assert args != null && args.length == 1 && args[0] instanceof GridCacheQueueType :
                        "Invalid query reducer argument: " + Arrays.toString(args);

                    return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>() {
                        private final Collection<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> items =
                            new ConcurrentLinkedQueue<>();

                        private final GridCacheQueueType type = (GridCacheQueueType)args[0];

                        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> e) {
                            if (e != null)
                                items.add(e);

                            return true;
                        }

                        @Override public Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> apply() {
                            if (items.isEmpty())
                                return null;

                            switch (type) {
                                case FIFO:
                                    return Collections.min(items, seqComp);
                                case LIFO:
                                    return Collections.max(items, seqComp);
                                case PRIORITY:
                                    return Collections.min(items, priComp);
                                default:
                                    assert false : "Unknown queue type: " + type;

                                    return null;
                            }
                        }
                    };
                }
            });

            lastItemQry.remoteReducer(rdcOneRecord);

            lastItemQry.localReducer(new C1<Object[], GridReducer<Map.Entry<GridCacheQueueItemKey,
                GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>>() {
                @Override public GridReducer<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                    Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> apply(final Object[] args) {
                    assert args != null && args.length == 1 && args[0] instanceof GridCacheQueueType
                        : "Invalid query reducer argument: " + Arrays.toString(args);

                    return new R1<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>,
                        Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>() {
                        private final Collection<Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> items =
                            new ConcurrentLinkedQueue<>();

                        private final GridCacheQueueType type = (GridCacheQueueType)args[0];

                        @Override public boolean collect(Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> e) {
                            if (e != null)
                                items.add(e);

                            return true;
                        }

                        @Override public Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>> apply() {
                            if (items.isEmpty())
                                return null;

                            switch (type) {
                                case FIFO:
                                    return Collections.max(items, seqComp);
                                case LIFO:
                                    return Collections.min(items, seqComp);
                                case PRIORITY:
                                    return Collections.max(items, priComp);
                                default:
                                    assert false : "Unknown queue type: " + type;

                                    return null;
                            }
                        }
                    };
                }
            });
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
     * @return Cache query for requesting all queue keys of collection of queue item.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
        GridBiTuple<Integer, GridException>> itemsKeysQuery(GridCacheQueueType type) {
        return rmvItemsQrys.get(type);
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting all queue keys.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, GridBiTuple<Integer, GridException>,
        GridBiTuple<Integer, GridException>> removeAllKeysQuery(GridCacheQueueType type) {
        return rmvAllKeysQrys.get(type);
    }

    /**
     * @param type Queue type.
     * @return Cache query for checking contains queue item.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, boolean[], Boolean>
    containsQuery(GridCacheQueueType type) {
        return containsQrys.get(type);
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting all queue items.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>>
        firstItemQuery(GridCacheQueueType type) {
        return firstItemQrys.get(type);
    }

    /**
     * @param type Queue type.
     * @return Cache query for requesting all queue items.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Map.Entry<GridCacheQueueItemKey,
        GridCacheQueueItemImpl<T>>, Map.Entry<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>>> lastItemQuery(
        GridCacheQueueType type) {
        return lastItemQrys.get(type);
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
     * @return Cache query for requesting item position.
     */
    GridCacheReduceQuery<GridCacheQueueItemKey, GridCacheQueueItemImpl<T>, Integer, Integer> itemPositionQuery(
        GridCacheQueueType type) {
        return posOfItemQrys.get(type);
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
}
