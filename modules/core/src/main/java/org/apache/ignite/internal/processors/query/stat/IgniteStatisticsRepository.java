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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnLocalDataViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnPartitionDataViewWalker;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.view.ColumnLocalDataViewSupplier;
import org.apache.ignite.internal.processors.query.stat.view.ColumnPartitionDataViewSupplier;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Statistics repository implementation. Store all statistics data (except configuration) and offer high level
 * operations to transform it.
 */
public class IgniteStatisticsRepository {
    /** Statistics partition data view name. */
    public static final String STAT_PART_DATA_VIEW = "statisticsPartitionData";

    /** Statistics partition data view description. */
    public static final String STAT_PART_DATA_VIEW_DESC = "Statistics per partition data.";

    /** Statistics local data view name. */
    public static final String STAT_LOCAL_DATA_VIEW = "statisticsLocalData";

    /** Statistics local data view description. */
    public static final String STAT_LOCAL_DATA_VIEW_DESC = "Statistics local node data.";

    /** Logger. */
    private final IgniteLogger log;

    /** Statistics store. */
    private final IgniteStatisticsStore store;

    /** Local (for current node) object statistics. */
    private final Map<StatisticsKey, VersionedStatistics> locStats = new ConcurrentHashMap<>();

    /** Obsolescence for each partition. */
    private final Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> statObs = new ConcurrentHashMap<>();

    /** Statistics helper (msg converter). */
    private final IgniteStatisticsHelper helper;

    /** Local statistics subscribers. */
    private final List<Consumer<ObjectStatisticsEvent>> subscribers = new CopyOnWriteArrayList<>();

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
     * Get partition obsolescence info.
     *
     * @param key Statistics key.
     * @param partId Parititon id.
     * @return Partition obsolescence info or {@code null} if it doesn't exist.
     */
    public ObjectPartitionStatisticsObsolescence getObsolescence(StatisticsKey key, int partId) {
        IntMap<ObjectPartitionStatisticsObsolescence> objObs = statObs.get(key);

        if (objObs == null)
            return null;

        return objObs.get(partId);
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
     * Save local object statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     * @param topVer Topology version.
     */
    public void saveLocalStatistics(
        StatisticsKey key,
        ObjectStatisticsImpl statistics,
        AffinityTopologyVersion topVer
    ) {
        locStats.put(key, new VersionedStatistics(topVer, statistics));

        ObjectStatisticsEvent newLocalStat = new ObjectStatisticsEvent(key, statistics, topVer);

        for (Consumer<ObjectStatisticsEvent> subscriber : subscribers)
            subscriber.accept(newLocalStat);
    }

    /**
     * Clear specified partition ids statistics.
     *
     * @param key Key to remove statistics by.
     * @param partsToRemove Set of parititon ids to remove.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key, Set<Integer> partsToRemove) {
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
     * @param topVer Required topology version.
     * @return Object local statistics or {@code null} if there are no statistics collected for such object.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key, AffinityTopologyVersion topVer) {
        VersionedStatistics stat = locStats.get(key);

        if (stat == null)
            return null;

        return (topVer != null && !stat.topologyVersion().equals(topVer)) ? null : stat.statistics();
    }

    /**
     * Get all local statistics. Return internal map without copying.
     *
     * @return Local (for current node) object statistics.
     */
    public Map<StatisticsKey, VersionedStatistics> localStatisticsMap() {
        return locStats;
    }

    /**
     * @return Ignite statistics store.
     */
    public IgniteStatisticsStore statisticsStore() {
        return store;
    }

    /**
     * Scan local partitioned statistic and aggregate local statistic for specified statistic object.
     *
     * @param stats Partitions statistics to aggregate.
     * @param cfg Statistic configuration to specify statistic object to aggregate.
     * @param topVer Topology version to which specified partition set actual for.
     * @return aggregated local statistic.
     */
    public ObjectStatisticsImpl aggregatedLocalStatistics(
        Collection<ObjectPartitionStatisticsImpl> stats,
        StatisticsObjectConfiguration cfg,
        AffinityTopologyVersion topVer
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Refresh local aggregated statistic [cfg=" + cfg +
                ", part.size()=" + stats.size() + ']');
        }

        ObjectStatisticsImpl locStat = helper.aggregateLocalStatistics(cfg, stats);

        if (locStat != null)
            saveLocalStatistics(cfg.key(), locStat, topVer);

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
     * Get list of all obsolescence keys.
     *
     * @return List of all obsolescence keys.
     */
    public synchronized List<StatisticsKey> getObsolescenceKeys() {
        return new ArrayList<>(statObs.keySet());
    }

    /**
     * Get map partitionId to partition obsolescence by key.
     *
     * @param key Statistics key to get obsolescence info by.
     * @return Obsolescence map.
     */
    public synchronized IntMap<ObjectPartitionStatisticsObsolescence> getObsolescence(StatisticsKey key) {
        IntMap<ObjectPartitionStatisticsObsolescence> res = statObs.get(key);

        if (res == null)
            return new IntHashMap<>(0);
        else
            return new IntHashMap<>(res);
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

    /**
     * Subscribe to all local statistics changes.
     *
     * @param subscriber Local statitics subscriber.
     */
    public void subscribeToLocalStatistics(Consumer<ObjectStatisticsEvent> subscriber
    ) {
        subscribers.add(subscriber);
    }

    /**
     * Object statistics with topology version to which it is actual for.
     */
    public static class VersionedStatistics {
        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Statistics. */
        private final ObjectStatisticsImpl statistics;

        /**
         * Constructor.
         *
         * @param topVer Topology version.
         * @param statistics Statistics.
         */
        public VersionedStatistics(AffinityTopologyVersion topVer, ObjectStatisticsImpl statistics) {
            this.topVer = topVer;
            this.statistics = statistics;
        }

        /**
         * @return Topology version.
         */
        public AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @return Statistics.
         */
        public ObjectStatisticsImpl statistics() {
            return statistics;
        }
    }
}
