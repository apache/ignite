/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Bundled factory for cache updaters.
 */
public class GridDataLoadCacheUpdaters {
    /** */
    private static final GridDataLoadCacheUpdater INDIVIDUAL = new Individual();

    /** */
    private static final GridDataLoadCacheUpdater BATCHED = new Batched();

    /** */
    private static final GridDataLoadCacheUpdater BATCHED_SORTED = new BatchedSorted();

    /** */
    private static final GridDataLoadCacheUpdater GROUP_LOCKED = new GroupLocked();

    /**
     * Updates cache using independent {@link GridCache#put(Object, Object, GridPredicate[])} and
     * {@link GridCache#remove(Object, GridPredicate[])} operations. Thus it is safe from deadlocks but performance
     * is not the best.
     *
     * @return Single updater.
     */
    public static <K, V> GridDataLoadCacheUpdater<K, V> individual() {
        return INDIVIDUAL;
    }

    /**
     * Updates cache using batched methods {@link GridCache#putAll(Map, GridPredicate[])} and
     * {@link GridCache#removeAll(Collection, GridPredicate[])}. Can cause deadlocks if the same keys are getting
     * updated concurrently. Performance is generally better than in {@link #individual()}.
     *
     * @return Batched updater.
     */
    public static <K, V> GridDataLoadCacheUpdater<K, V> batched() {
        return BATCHED;
    }

    /**
     * Updates cache using batched methods {@link GridCache#putAll(Map, GridPredicate[])} and
     * {@link GridCache#removeAll(Collection, GridPredicate[])}. Keys are sorted in natural order and if all updates
     * use the same rule deadlock can not happen. Performance is generally better than in {@link #individual()}.
     *
     * @return Batched sorted updater.
     */
    public static <K extends Comparable<?>, V> GridDataLoadCacheUpdater<K, V> batchedSorted() {
        return BATCHED_SORTED;
    }

    /**
     * Updates cache using batched methods {@link GridCache#putAll(Map, GridPredicate[])} and
     * {@link GridCache#removeAll(Collection, GridPredicate[])} in group lock transaction. Requires that there are no
     * concurrent updates other than in group lock.
     *
     * @return Updater with group lock.
     */
    public static <K, V> GridDataLoadCacheUpdater<K, V> groupLocked() {
        return GROUP_LOCKED;
    }

    /**
     * Updates cache.
     *
     * @param cache Cache.
     * @param rmvCol Keys to remove.
     * @param putMap Entries to put.
     * @throws GridException If failed.
     */
    protected static <K, V> void updateAll(GridCacheProjection<K,V> cache, @Nullable Collection<K> rmvCol,
        Map<K, V> putMap) throws GridException {
        assert rmvCol != null || putMap != null;

        // Here we assume that there are no key duplicates, so the following calls are valid.
        if (rmvCol != null)
            cache.removeAll(rmvCol);

        if (putMap != null)
            cache.putAll(putMap);
    }

    /**
     * Simple cache updater implementation. Updates keys one by one thus is not dead lock prone.
     */
    private static class Individual<K, V> implements GridDataLoadCacheUpdater<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void update(GridCache<K, V> cache, Collection<Map.Entry<K, V>> entries)
            throws GridException {
            assert cache != null;
            assert !F.isEmpty(entries);

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                if (val == null)
                    cache.removex(key);
                else
                    cache.putx(key, val);
            }
        }
    }

    /**
     * Batched updater. Updates cache using batch operations thus is dead lock prone.
     */
    private static class Batched<K, V> implements GridDataLoadCacheUpdater<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void update(GridCache<K, V> cache, Collection<Map.Entry<K, V>> entries)
            throws GridException {
            assert cache != null;
            assert !F.isEmpty(entries);

            Map<K, V> putAll = null;
            Collection<K> rmvAll = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                if (val == null) {
                    if (rmvAll == null)
                        rmvAll = new ArrayList<>();

                    rmvAll.add(key);
                }
                else {
                    if (putAll == null)
                        putAll = new HashMap<>();

                    putAll.put(key, val);
                }
            }

            updateAll(cache, rmvAll, putAll);
        }
    }

    /**
     * Batched updater. Updates cache using batch operations thus is dead lock prone.
     */
    private static class BatchedSorted<K, V> implements GridDataLoadCacheUpdater<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void update(GridCache<K, V> cache, Collection<Map.Entry<K, V>> entries)
            throws GridException {
            assert cache != null;
            assert !F.isEmpty(entries);

            Map<K, V> putAll = null;
            Collection<K> rmvAll = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key instanceof Comparable;

                V val = entry.getValue();

                if (val == null) {
                    if (rmvAll == null)
                        rmvAll = new TreeSet<>();

                    rmvAll.add(key);
                }
                else {
                    if (putAll == null)
                        putAll = new TreeMap<>();

                    putAll.put(key, val);
                }
            }

            updateAll(cache, rmvAll, putAll);
        }
    }

    /**
     * Cache updater which uses group lock.
     */
    private static class GroupLocked<K, V> implements GridDataLoadCacheUpdater<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void update(GridCache<K, V> cache, Collection<Map.Entry<K, V>> entries)
            throws GridException {
            assert cache != null;
            assert !F.isEmpty(entries);

            assert cache.configuration().getAtomicityMode() != ATOMIC;

            Map<Integer, Integer> partsCounts = new HashMap<>();

            // Group by partition ID.
            Map<Integer, Collection<K>> rmvPartMap = null;
            Map<Integer, Map<K, V>> putPartMap = null;

            for (Map.Entry<K, V> entry : entries) {
                K key = entry.getKey();

                assert key != null;

                V val = entry.getValue();

                int part = cache.affinity().partition(key);

                Integer cnt = partsCounts.get(part);

                partsCounts.put(part, cnt == null ? 1 : cnt + 1);

                if (val == null) {
                    if (rmvPartMap == null)
                        rmvPartMap = new HashMap<>();

                    F.addIfAbsent(rmvPartMap, part, F.<K>newList()).add(key);
                }
                else {
                    if (putPartMap == null)
                        putPartMap = new HashMap<>();

                    F.addIfAbsent(putPartMap, part, F.<K, V>newMap()).put(key, val);
                }
            }

            for (Map.Entry<Integer, Integer> e : partsCounts.entrySet()) {
                Integer part = e.getKey();
                int cnt = e.getValue();

                GridCacheTx tx = cache.txStartPartition(part, PESSIMISTIC, REPEATABLE_READ, 0, cnt);

                try {
                    updateAll(cache, rmvPartMap == null ? null : rmvPartMap.get(part),
                        putPartMap == null ? null : putPartMap.get(part));

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }
        }
    }
}
