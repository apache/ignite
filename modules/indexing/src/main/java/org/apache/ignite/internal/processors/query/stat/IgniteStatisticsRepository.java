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
import java.util.Collections;
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
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnConfigurationView;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnLocalDataView;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnPartitionDataView;
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
    private AtomicBoolean started = new AtomicBoolean(false);

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
        this.log = logSupplier.apply(IgniteStatisticsRepository.class);

        sysViewMgr.registerFiltrableView(STAT_PART_DATA_VIEW, STAT_PART_DATA_VIEW_DESC,
            new StatisticsColumnPartitionDataViewWalker(), this::columnPartitionStatisticsViewSupplier,
            Function.identity());

        sysViewMgr.registerFiltrableView(STAT_LOCAL_DATA_VIEW, STAT_LOCAL_DATA_VIEW_DESC,
            new StatisticsColumnLocalDataViewWalker(), this::columnLocalStatisticsViewSupplier,
            Function.identity());
    }

    /**
     * Statistics column partition data view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with statistics column partition data views.
     */
    private Iterable<StatisticsColumnPartitionDataView> columnPartitionStatisticsViewSupplier(
        Map<String, Object> filter
    ) {
        String type = (String)filter.get(StatisticsColumnPartitionDataViewWalker.TYPE_FILTER);
        if (type != null && !StatisticsColumnConfigurationView.TABLE_TYPE.equalsIgnoreCase(type))
            return Collections.emptyList();

        String schema = (String)filter.get(StatisticsColumnPartitionDataViewWalker.SCHEMA_FILTER);
        String name = (String)filter.get(StatisticsColumnPartitionDataViewWalker.NAME_FILTER);
        Integer partId = (Integer)filter.get(StatisticsColumnPartitionDataViewWalker.PARTITION_FILTER);
        String column = (String)filter.get(StatisticsColumnPartitionDataViewWalker.COLUMN_FILTER);

        Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> partsStatsMap;
        if (!F.isEmpty(schema) && !F.isEmpty(name)) {
            StatisticsKey key = new StatisticsKey(schema, name);
            Collection<ObjectPartitionStatisticsImpl> keyStat;
            if (partId == null)
                keyStat = store.getLocalPartitionsStatistics(key);
            else {
                ObjectPartitionStatisticsImpl partStat = store.getLocalPartitionStatistics(key, partId);
                keyStat = (partStat == null) ? Collections.emptyList() : Collections.singletonList(partStat);
            }
            partsStatsMap = Collections.singletonMap(key, keyStat);
        }
        else
            partsStatsMap = store.getAllLocalPartitionsStatistics(schema);

        List<StatisticsColumnPartitionDataView> res = new ArrayList<>();

        for (Map.Entry<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> partsStatsEntry : partsStatsMap.entrySet()) {
            StatisticsKey key = partsStatsEntry.getKey();
            for (ObjectPartitionStatisticsImpl partStat : partsStatsEntry.getValue()) {
                if (column == null) {
                    for (Map.Entry<String, ColumnStatistics> colStatEntry : partStat.columnsStatistics().entrySet())
                        res.add(new StatisticsColumnPartitionDataView(key, colStatEntry.getKey(), partStat));
                }
                else {
                    ColumnStatistics colStat = partStat.columnStatistics(column);
                    if (colStat != null)
                        res.add(new StatisticsColumnPartitionDataView(key, column, partStat));
                }
            }
        }

        return res;
    }

    /**
     * Statistics column local node data view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with statistics column local node data views.
     */
    private Iterable<StatisticsColumnLocalDataView> columnLocalStatisticsViewSupplier(Map<String, Object> filter) {
        String type = (String)filter.get(StatisticsColumnPartitionDataViewWalker.TYPE_FILTER);
        if (type != null && !StatisticsColumnConfigurationView.TABLE_TYPE.equalsIgnoreCase(type))
            return Collections.emptyList();

        String schema = (String)filter.get(StatisticsColumnLocalDataViewWalker.SCHEMA_FILTER);
        String name = (String)filter.get(StatisticsColumnLocalDataViewWalker.NAME_FILTER);
        String column = (String)filter.get(StatisticsColumnPartitionDataViewWalker.COLUMN_FILTER);

        Map<StatisticsKey, ObjectStatisticsImpl> localStatsMap;
        if (!F.isEmpty(schema) && !F.isEmpty(name)) {
            StatisticsKey key = new StatisticsKey(schema, name);

            ObjectStatisticsImpl objLocalStat = locStats.get(key);

            if (objLocalStat == null)
                return Collections.emptyList();

            localStatsMap = Collections.singletonMap(key, objLocalStat);
        }
        else
            localStatsMap = locStats.entrySet().stream().filter(e -> F.isEmpty(schema) || schema.equals(e.getKey().schema()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<StatisticsColumnLocalDataView> res = new ArrayList<>();

        for (Map.Entry<StatisticsKey, ObjectStatisticsImpl> localStatsEntry : localStatsMap.entrySet()) {
            StatisticsKey key = localStatsEntry.getKey();
            ObjectStatisticsImpl stat = localStatsEntry.getValue();
            if (column == null) {
                for (Map.Entry<String, ColumnStatistics> colStat : localStatsEntry.getValue().columnsStatistics()
                    .entrySet()) {
                    StatisticsColumnLocalDataView colStatView = new StatisticsColumnLocalDataView(key, colStat.getKey(),
                        stat);

                    res.add(colStatView);
                }
            }
            else {
                ColumnStatistics colStat = localStatsEntry.getValue().columnStatistics(column);
                if (colStat != null) {
                    StatisticsColumnLocalDataView colStatView = new StatisticsColumnLocalDataView(key, column, stat);

                    res.add(colStatView);
                }
            }
        }

        return res;
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
     * Clear partition statistics for specified object.
     *
     * @param key Object to clear statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key, Set<String> colNames) {
        if (F.isEmpty(colNames)) {
            store.clearLocalPartitionsStatistics(key);

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
            ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
            store.saveLocalPartitionStatistics(key, combinedStats);
        }
    }

    /**
     * Refresh statistics obsolescence after partition gathering.
     *
     * @param key Statistics key.
     * @param partId Partition id.
     */
    public void refreshObsolescence(StatisticsKey key, int partId) {
        statObs.compute(key, (k, v) -> {
            if (v == null)
                v = new IntHashMap<>();

            ObjectPartitionStatisticsObsolescence newObs = new ObjectPartitionStatisticsObsolescence();
            newObs.dirty(true);

            v.put(partId, newObs);

            return v;
        });
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
     * Get local statistics.
     *
     * @param key Object key to load statistics by.
     * @return Object local statistics or {@code null} if there are no statistics collected for such object.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key) {
        return locStats.get(key);
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
     * Add new statistics into base one (with overlapping of existing data).
     *
     * @param base Old statistics.
     * @param add Updated statistics.
     * @param <T> Statistics type (partition or object one).
     * @return Combined statistics.
     */
    public static <T extends ObjectStatisticsImpl> T add(T base, T add) {
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
     * @param parts Partitions numbers to aggregate,
     * @param cfg Statistic configuration to specify statistic object to aggregate.
     * @return aggregated local statistic.
     */
    public ObjectStatisticsImpl aggregatedLocalStatistics(
        Set<Integer> parts,
        StatisticsObjectConfiguration cfg
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Refresh local aggregated statistic [cfg=" + cfg +
                ", part=" + parts + ']');
        }

        Collection<ObjectPartitionStatisticsImpl> stats = store.getLocalPartitionsStatistics(cfg.key());

        Collection<ObjectPartitionStatisticsImpl> statsToAgg = stats.stream()
            .filter(s -> parts.contains(s.partId()))
            .collect(Collectors.toList());

        assert statsToAgg.size() == parts.size() : "Cannot aggregate local statistics: not enough partitioned statistics";

        ObjectStatisticsImpl locStat = helper.aggregateLocalStatistics(
            cfg,
            statsToAgg
        );

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
     * Update obsolescence info cache to fit specified cfg.
     *
     * @param cfg Obsolescence configuration.
     */
    public void updateObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        Map<StatisticsKey, Set<Integer>> deleted = updateObsolescenceInfo(statObs, cfg);

        for (Map.Entry<StatisticsKey, Set<Integer>> objDeleted : deleted.entrySet())
            store.clearObsolescenceInfo(objDeleted.getKey(), objDeleted.getValue());
    }

    /**
     * Load or update obsolescence info cache to fit specified cfg.
     *
     * @param cfg Map object statistics configuration to primary partitions set.
     */
    public synchronized void checkObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        if (!started.compareAndSet(false, true))
            loadObsolescenceInfo(cfg);
        else
            updateObsolescenceInfo(cfg);
    }

    /**
     * Make obsolescence map correlated with configuration and return removed elements map.
     *
     * @param obsolescence Obsolescence info map.
     * @param cfg Obsolescence configuration.
     * @return Removed from obsolescence info map partitions.
     */
    private static Map<StatisticsKey, Set<Integer>> updateObsolescenceInfo(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence,
        Map<StatisticsObjectConfiguration, Set<Integer>> cfg
    ) {
        Map<StatisticsKey, Set<Integer>> res = new HashMap<>();

        Set<StatisticsKey> keys = cfg.keySet().stream().map(StatisticsObjectConfiguration::key).collect(Collectors.toSet());

        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : obsolescence.entrySet()) {
            StatisticsKey key = objObs.getKey();
            if (!keys.contains(key)) {
                IntMap<ObjectPartitionStatisticsObsolescence> rmv = obsolescence.remove(key);

                res.put(key, IntStream.of(rmv.keys()).boxed().collect(Collectors.toSet()));
            }
        }

        for (Map.Entry<StatisticsObjectConfiguration, Set<Integer>> objObsCfg : cfg.entrySet()) {
            StatisticsKey key = objObsCfg.getKey().key();
            IntMap<ObjectPartitionStatisticsObsolescence> objObs = obsolescence.get(objObsCfg.getKey().key());
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
    public void addRowsModified(StatisticsKey key, int partId, byte[] changedKey) {
        IntMap<ObjectPartitionStatisticsObsolescence> objObs = statObs.get(key);
        if (objObs == null)
            return;

        ObjectPartitionStatisticsObsolescence objPartObs = objObs.get(partId);
        if (objPartObs == null)
            return;

        objPartObs.modify(changedKey);
    }

    /**
     * Save all modified obsolescence info to local metastorage.
     *
     * @return Map with all partitions of objects with dirty partitions.
     */
    public synchronized Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> saveObsolescenceInfo() {
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirtyObs = new HashMap<>();

        boolean hasDirty[] = new boolean[1];
        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : statObs.entrySet()) {
            hasDirty[0] = false;

            objObs.getValue().forEach((k, v) -> {
                if (v.dirty()) {
                    v.dirty(false);
                    hasDirty[0] = true;
                }
            });

            if (hasDirty[0]) {
                IntMap<ObjectPartitionStatisticsObsolescence> objDirtyObs = new IntHashMap<>();

                objObs.getValue().forEach((k, v) -> objDirtyObs.put(k, v));

                dirtyObs.put(objObs.getKey(), objDirtyObs);
            }
        }

        store.saveObsolescenceInfo(dirtyObs);

        return dirtyObs;
    }

    /**
     * Remove statistics obsolescence info by the given key.
     *
     * @param key Statistics key to remove obsolescence info by.
     */
    public void removeObsolescenceInfo(StatisticsKey key) {
        statObs.remove(key);

        store.clearObsolescenceInfo(key, null);
    }
}
