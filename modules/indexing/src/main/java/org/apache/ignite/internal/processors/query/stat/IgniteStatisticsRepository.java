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
//import org.apache.ignite.internal.util.typedef.F;

/**
 * Statistics repository implementation. Store all statistics data (except configuration) and offer high level
 * operations to transform it.
 */
public class IgniteStatisticsRepository {
    /**
     *
     */
    public static final String STAT_PART_DATA_VIEW = "statisticsPartitionData";

    /**
     *
     */
    public static final String STAT_PART_DATA_VIEW_DESC = "Statistics per partition data.";

    /**
     *
     */
    public static final String STAT_LOCAL_DATA_VIEW = "statisticsLocalData";

    /**
     *
     */
    public static final String STAT_LOCAL_DATA_VIEW_DESC = "Statistics local node data.";

    /**
     * Logger.
     */
    private final IgniteLogger log;

    /**
     * Statistics store.
     */
    private final IgniteStatisticsStore store;

    /**
     * Local (for current node) object statistics.
     */
    private final Map<StatisticsKey, ObjectStatisticsImpl> locStats = new ConcurrentHashMap<>();

    /**
     * Obsolescence for each partition.
     */
    private final Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> statObs = new ConcurrentHashMap<>();

    /**
     * Statistics gathering.
     */
    private final IgniteStatisticsHelper helper;

    /**
     * Constructor.
     *
     * @param store       Ignite statistics store to use.
     * @param sysViewMgr  Grid system view manager.
     * @param helper      IgniteStatisticsHelper.
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
        if (F.isEmpty(partsToRemove)) {
            store.clearLocalPartitionsStatistics(key);
            store.clearObsolescenceInfo(key, null);
            locStats.remove(key);
            statObs.remove(key);
        }
        else {
            store.clearLocalPartitionsStatistics(key, partsToRemove);
            store.clearObsolescenceInfo(key, partsToRemove);

            statObs.computeIfPresent(key, (k, v) -> {
                for (Integer partToRemove : partsToRemove)
                    v.remove(partToRemove);

                return (v.isEmpty()) ? null : v;
            });
        }
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
    public Map<StatisticsKey, ObjectStatisticsImpl> localStatisticsMap() {
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

        Map<StatisticsKey, StatisticsObjectConfiguration> targetKeysMap = targetCfg.keySet().stream()
            .collect(Collectors.toMap(StatisticsObjectConfiguration::key, c -> c));

        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : obsolescence.entrySet()) {
            StatisticsKey key = objObs.getKey();

            StatisticsObjectConfiguration targetKeyCfg = targetKeysMap.get(key);

            if (targetKeyCfg == null) {
                // No target key at all - add all partitions to rmv map
                IntMap<ObjectPartitionStatisticsObsolescence> rmv = obsolescence.remove(key);

                res.put(key, IntStream.of(rmv.keys()).boxed().collect(Collectors.toSet()));
            }
            else {
                // Target key exists - check each partition id.
                Set<Integer> targetKeyParts = targetCfg.get(targetKeyCfg);

                for (int objPartId : objObs.getValue().keys()) {
                    if (!targetKeyParts.contains(objPartId))
                        res.putIfAbsent(key, new HashSet<Integer>()).add(objPartId);
                }
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

    public synchronized List<StatisticsKey> getObsolescenceKeys() {
        return new ArrayList<>(statObs.keySet());
    }

    public synchronized IntMap<ObjectPartitionStatisticsObsolescence> getObsolescence(StatisticsKey key) {
        IntMap<ObjectPartitionStatisticsObsolescence> res = statObs.get(key);

        if (res == null)
            return new IntHashMap<>(0);
        else
            return new IntHashMap<>(res);
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
