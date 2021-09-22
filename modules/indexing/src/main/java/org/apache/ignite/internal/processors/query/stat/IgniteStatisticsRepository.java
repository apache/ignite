/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnLocalDataViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnPartitionDataViewWalker;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.view.ColumnLocalDataViewSupplier;
import org.apache.ignite.internal.processors.query.stat.view.ColumnPartitionDataViewSupplier;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Statistics repository implementation.
 */
public class IgniteStatisticsRepository {
    /** */
    public static final String STAT_PART_DATA_VIEW = "statisticsPartitionData";

    /** */
    public static final String STAT_PART_DATA_VIEW_DESC = "Statistics per partition data.";

    /** */
    public static final String STAT_LOCAL_DATA_VIEW = "statisticsLocalData";

    /** */
    public static final String STAT_LOCAL_DATA_VIEW_DESC = "Statistics local node data.";

    /** Logger. */
    private final IgniteLogger log;

    /** Statistics store. */
    private final IgniteStatisticsStore store;

    /** Local (for current node) object statistics. */
    private final Map<StatisticsKey, ObjectStatisticsImpl> locStats = new ConcurrentHashMap<>();

    /** Obsolescence for each partition. */
    private final Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> statObs = new ConcurrentHashMap<>();

    /** Statistics gathering. */
    private final IgniteStatisticsHelper helper;

    /** Started flag. */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Constructor.
     *
     * @param store Ignite statistics store to use.
     * @param sysViewMgr Grid system view manager.
     * @param helper IgniteStatisticsHelper.
     * @param logSupplier Ignite logger supplier to get logger from.
     */
    public IgniteStatisticsRepository(
        IgniteStatisticsStore store,
        GridSystemViewManager sysViewMgr,
        IgniteStatisticsHelper helper,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.store = store;
        this.helper = helper;
        log = logSupplier.apply(IgniteStatisticsRepository.class);

        ColumnPartitionDataViewSupplier colPartDataViewSupplier = new ColumnPartitionDataViewSupplier(store);

        sysViewMgr.registerFiltrableView(STAT_PART_DATA_VIEW, STAT_PART_DATA_VIEW_DESC,
            new StatisticsColumnPartitionDataViewWalker(),
            colPartDataViewSupplier::columnPartitionStatisticsViewSupplier,
            Function.identity());

        ColumnLocalDataViewSupplier colLocDataViewSupplier = new ColumnLocalDataViewSupplier(this);

        sysViewMgr.registerFiltrableView(STAT_LOCAL_DATA_VIEW, STAT_LOCAL_DATA_VIEW_DESC,
            new StatisticsColumnLocalDataViewWalker(),
            colLocDataViewSupplier::columnLocalStatisticsViewSupplier,
            Function.identity());
    }

    /**
     * Get local partition statistics by specified object.
     *
     * @param key Object to get statistics by.
     * @return Collection of partitions statistics.
     */
    public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        return store.getLocalPartitionsStatistics(key);
    }

    /**
     * Clear partition statistics and obsolescence info for specified object.
     *
     * @param key Object to clear statistics and obsolescence by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key, Set<String> colNames) {
        if (F.isEmpty(colNames)) {
            store.clearLocalPartitionsStatistics(key);
            store.clearObsolescenceInfo(key, null);
            locStats.remove(key);
            statObs.remove(key);
            return;
        }

        Collection<ObjectPartitionStatisticsImpl> oldStatistics = store.getLocalPartitionsStatistics(key);

        if (oldStatistics.isEmpty())
            return;

        Collection<ObjectPartitionStatisticsImpl> newStatistics = new ArrayList<>(oldStatistics.size());
        Collection<Integer> partitionsToRmv = new ArrayList<>();

        for (ObjectPartitionStatisticsImpl oldStat : oldStatistics) {
            ObjectPartitionStatisticsImpl newStat = subtract(oldStat, colNames);

            if (!newStat.columnsStatistics().isEmpty())
                newStatistics.add(newStat);
            else
                partitionsToRmv.add(oldStat.partId());
        }

        if (newStatistics.isEmpty()) {
            store.clearLocalPartitionsStatistics(key);
            store.clearObsolescenceInfo(key, null);

            return;
        }

        if (!partitionsToRmv.isEmpty()) {
            store.clearLocalPartitionsStatistics(key, partitionsToRmv);
            store.clearObsolescenceInfo(key, partitionsToRmv);
        }

        store.replaceLocalPartitionsStatistics(key, newStatistics);
    }

    /**
     * Save specified local partition statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalPartitionStatistics(
        StatisticsKey key,
        ObjectPartitionStatisticsImpl statistics
    ) {
        ObjectPartitionStatisticsImpl oldPartStat = store.getLocalPartitionStatistics(key, statistics.partId());

        if (oldPartStat == null)
            store.saveLocalPartitionStatistics(key, statistics);
        else {
            ObjectPartitionStatisticsImpl combinedStats = merge(oldPartStat, statistics);
            store.saveLocalPartitionStatistics(key, combinedStats);
        }
    }

    /**
     * Refresh statistics obsolescence and save clear object to store, after partition gathering.
     *
     * @param key Statistics key.
     * @param partId Partition id.
     */
    public void refreshObsolescence(StatisticsKey key, int partId) {
        ObjectPartitionStatisticsObsolescence newObs = new ObjectPartitionStatisticsObsolescence();
        newObs.dirty(false);

        statObs.compute(key, (k, v) -> {
            if (v == null)
                v = new IntHashMap<>();

            v.put(partId, newObs);

            return v;
        });

        store.saveObsolescenceInfo(key, partId, newObs);
    }

    /**
     * Save specified local partition statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void replaceLocalPartitionStatistics(
        StatisticsKey key,
        ObjectPartitionStatisticsImpl statistics
    ) {
        store.saveLocalPartitionStatistics(key, statistics);
    }

    /**
     * Replace all object statistics with specified ones.
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     */
    public void saveLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        store.replaceLocalPartitionsStatistics(key, statistics);
    }

    /**
     * Get partition statistics.
     *
     * @param key Object key.
     * @param partId Partition id.
     * @return Object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        return store.getLocalPartitionStatistics(key, partId);
    }

    /**
     * Clear partition statistics.
     *
     * @param key Object key.
     * @param partId Partition id.
     */
    public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        store.clearLocalPartitionStatistics(key, partId);

    }

    /**
     * Save local object statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        locStats.put(key, statistics);
    }

    /**
     * Clear specified partition ids statistics.
     *
     * @param key Key to remove statistics by.
     * @param partsToRemove Set of parititon ids to remove.
     */
    public void clearLocalPartitionIdsStatistics(StatisticsKey key, Set<Integer> partsToRemove) {
        store.clearLocalPartitionsStatistics(key, partsToRemove);

        statObs.computeIfPresent(key, (k, v) -> {
            for (Integer partToRemove : partsToRemove)
                v.remove(partToRemove);

            return (v.isEmpty()) ? null : v;
        });
    }

    /**
     * Get local statistics.
     *
     * @param key Object key to load statistics by.
     * @return Object local statistics or {@code null} if there are no statistics collected for such object.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key) {
        return locStats.get(key);
    }

    /**
     * Get all local statistics. Return internal map without copying.
     *
     * @return Local (for current node) object statistics.
     */
    public Map<StatisticsKey, ObjectStatisticsImpl> getAllLocalStatisticsInt() {
        return locStats;
    }

    /**
     * Clear local object statistics.
     *
     * @param key Object key to clear local statistics by.
     */
    public void clearLocalStatistics(StatisticsKey key) {
        locStats.remove(key);
    }

    /**
     * Clear local object statistics.
     *
     * @param key Object key to clear local statistics by.
     * @param colNames Only statistics by specified columns will be cleared.
     */
    public void clearLocalStatistics(StatisticsKey key, Set<String> colNames) {
        if (log.isDebugEnabled()) {
            log.debug("Clear local statistics [key=" + key +
                ", columns=" + colNames + ']');
        }

        if (locStats == null) {
            log.warning("Unable to clear local statistics for " + key + " on non server node.");

            return;
        }

        locStats.computeIfPresent(key, (k, v) -> {
            ObjectStatisticsImpl locStatNew = subtract(v, colNames);
            return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
        });
    }

    /**
     * @return Ignite statistics store.
     */
    public IgniteStatisticsStore statisticsStore() {
        return store;
    }

    /**
     * Merge new statistics into base one (with overlapping of existing data).
     *
     * @param base Old statistics.
     * @param add Updated statistics.
     * @param <T> Statistics type (partition or object one).
     * @return Combined statistics.
     */
    public static <T extends ObjectStatisticsImpl> T merge(T base, T add) {
        T res = (T)add.clone();

        for (Map.Entry<String, ColumnStatistics> entry : base.columnsStatistics().entrySet())
            res.columnsStatistics().putIfAbsent(entry.getKey(), entry.getValue());

        return res;
    }

    /**
     * Remove specified columns from clone of base ObjectStatistics object.
     *
     * @param base ObjectStatistics to remove columns from.
     * @param cols Columns to remove.
     * @return Cloned object without specified columns statistics.
     */
    public static <T extends ObjectStatisticsImpl> T subtract(T base, Set<String> cols) {
        T res = (T)base.clone();

        for (String col : cols)
            res.columnsStatistics().remove(col);

        return res;
    }

    /**
     * Scan local partitioned statistic and aggregate local statistic for specified statistic object.
     *
     * @param stats Partitions statistics to aggregate.
     * @param cfg Statistic configuration to specify statistic object to aggregate.
     * @return aggregated local statistic.
     */
    public ObjectStatisticsImpl aggregatedLocalStatistics(
        Collection<ObjectPartitionStatisticsImpl> stats,
        StatisticsObjectConfiguration cfg
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Refresh local aggregated statistic [cfg=" + cfg +
                ", part.size()=" + stats.size() + ']');
        }

        ObjectStatisticsImpl locStat = helper.aggregateLocalStatistics(cfg, stats);

        if (locStat != null)
            saveLocalStatistics(cfg.key(), locStat);

        return locStat;
    }

    /**
     * Stop repository.
     */
    public synchronized void stop() {
        locStats.clear();
        started.set(false);

        if (log.isDebugEnabled())
            log.debug("Statistics repository started.");
    }

    /**
     * Start repository.
     */
    public synchronized void start() {
        if (log.isDebugEnabled())
            log.debug("Statistics repository started.");
    }

    /**
     * Load obsolescence info from local metastorage and cache it. Remove parts, that doesn't match configuration.
     * Create missing partitions info.
     *
     * @param cfg Partitions configuration.
     */
    public void loadObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence = store.loadAllObsolescence();

        Map<StatisticsKey, Set<Integer>> deleted = updateObsolescenceInfo(obsolescence, cfg);

        statObs.putAll(obsolescence);

        for (Map.Entry<StatisticsKey, Set<Integer>> objDeleted : deleted.entrySet())
            store.clearObsolescenceInfo(objDeleted.getKey(), objDeleted.getValue());
    }

    /**
     * Update obsolescence info cache to fit specified cfg. Remove the others from store to clean it.
     *
     * @param cfg Obsolescence configuration.
     */
    public void updateObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        Map<StatisticsKey, Set<Integer>> deleted = updateObsolescenceInfo(statObs, cfg);

        for (Map.Entry<StatisticsKey, Set<Integer>> objDeleted : deleted.entrySet())
            store.clearObsolescenceInfo(objDeleted.getKey(), objDeleted.getValue());

        fitObsolescenceInfo(cfg);
    }

    /**
     * Check store to clean unnecessary records.
     *
     * @param cfg Map object statistics configuration to primary partitions set.
     */
    private void fitObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        Map<StatisticsKey, Set<Integer>> keyPartCfg = new HashMap<>(cfg.size());
        cfg.forEach((k, v) -> keyPartCfg.put(k.key(), v));

        Map<StatisticsKey, Collection<Integer>> obsolescenceMap = store.loadObsolescenceMap();

        for (Map.Entry<StatisticsKey, Collection<Integer>> objObs : obsolescenceMap.entrySet()) {
            Set<Integer> cfgObs = keyPartCfg.get(objObs.getKey());

            if (cfgObs == null)
                store.clearObsolescenceInfo(objObs.getKey(), null);
            else {
                List<Integer> partToRemove = new ArrayList<>();

                for (Integer objPartObsId :objObs.getValue()) {
                    if (!cfgObs.contains(objPartObsId))
                        partToRemove.add(objPartObsId);
                }

                store.clearObsolescenceInfo(objObs.getKey(), partToRemove);
            }
        }
    }

    /**
     * Make obsolescence map correlated with configuration and return removed elements map.
     * Save new obsolescence info if necessary.
     *
     * @param obsolescence Obsolescence info map.
     * @param targetCfg Obsolescence configuration.
     * @return Removed from obsolescence info map partitions.
     */
    private static Map<StatisticsKey, Set<Integer>> updateObsolescenceInfo(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence,
        Map<StatisticsObjectConfiguration, Set<Integer>> targetCfg
    ) {
        Map<StatisticsKey, Set<Integer>> res = new HashMap<>();

        Set<StatisticsKey> targetKeys = targetCfg.keySet().stream().map(StatisticsObjectConfiguration::key)
            .collect(Collectors.toSet());

        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : obsolescence.entrySet()) {
            StatisticsKey key = objObs.getKey();

            if (!targetKeys.contains(key)) {
                IntMap<ObjectPartitionStatisticsObsolescence> rmv = obsolescence.remove(key);

                res.put(key, IntStream.of(rmv.keys()).boxed().collect(Collectors.toSet()));
            }
        }

        for (Map.Entry<StatisticsObjectConfiguration, Set<Integer>> objObsCfg : targetCfg.entrySet()) {
            StatisticsKey key = objObsCfg.getKey().key();
            IntMap<ObjectPartitionStatisticsObsolescence> objObs = obsolescence.get(key);

            if (objObs == null) {
                objObs = new IntHashMap<>();

                obsolescence.put(key, objObs);
            }

            IntMap<ObjectPartitionStatisticsObsolescence> objObsFinal = objObs;

            Set<Integer> partIds = new HashSet<>(objObsCfg.getValue());

            objObs.forEach((partId, v) -> {
                if (!partIds.remove(partId)) {
                    objObsFinal.remove(partId);

                    res.computeIfAbsent(key, k -> new HashSet<>()).add(partId);
                }
            });

            partIds.forEach(partId -> objObsFinal.put(partId, new ObjectPartitionStatisticsObsolescence()));
        }

        return res;
    }

    /**
     * Try to count modified to specified object and partition.
     *
     * @param key Statistics key.
     * @param partId Partition id.
     * @param changedKey Changed key bytes.
     */
    public void onRowModified(StatisticsKey key, int partId, byte[] changedKey) {
        IntMap<ObjectPartitionStatisticsObsolescence> objObs = statObs.get(key);

        if (objObs == null)
            return;

        ObjectPartitionStatisticsObsolescence objPartObs = objObs.get(partId);

        if (objPartObs == null)
            return;

        objPartObs.onModified(changedKey);
    }

    /**
     * Get all dirty partityions map.
     *
     * @return Map with all dirty partitions.
     */
    public synchronized Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> getDirtyObsolescenceInfo() {
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirtyObs = new HashMap<>();

        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : statObs.entrySet()) {
            objObs.getValue().forEach((k, v) -> {
                if (v.dirty())
                    dirtyObs.computeIfAbsent(objObs.getKey(), k2 -> new IntHashMap()).put(k, v);

            });
        }

        return dirtyObs;
    }

    /**
     * Save obsolescence info by specified key. Reset dirty flags.
     *
     * @param key Key to save obsolescence info by.
     */
    public void saveObsolescenceInfo(StatisticsKey key) {
        IntMap<ObjectPartitionStatisticsObsolescence> objObs = statObs.get(key);

        if (objObs == null)
            return;

        objObs.forEach((k, v) -> {
            if (v.dirty()) {
                store.saveObsolescenceInfo(key, k, v);

                v.dirty(false);
            }
        });
    }
}
