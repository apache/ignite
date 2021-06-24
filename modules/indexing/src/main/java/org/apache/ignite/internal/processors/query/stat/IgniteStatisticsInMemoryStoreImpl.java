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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Sql statistics storage in metastore.
 * Will store all statistics related objects with prefix "stats."
 * Store only partition level statistics.
 */
public class IgniteStatisticsInMemoryStoreImpl implements IgniteStatisticsStore {
    /** Key -> Partition -> Partition Statistics map, populated only on server nodes without persistence enabled. */
    private final Map<StatisticsKey, IntMap<ObjectPartitionStatisticsImpl>> partsStats = new ConcurrentHashMap<>();

    /** Key -> Partition -> ObsolescenceInfo map, populated only on server nodes without persistence enabled.  */
    private final Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsStats = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param logSupplier Logger getting function.
     */
    public IgniteStatisticsInMemoryStoreImpl(Function<Class<?>, IgniteLogger> logSupplier) {
        this.log = logSupplier.apply(IgniteStatisticsInMemoryStoreImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void clearAllStatistics() {
        partsStats.clear();
        obsStats.clear();
    }

    /** {@inheritDoc} */
    @Override public Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> getAllLocalPartitionsStatistics(
        String schema
    ) {
        Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> res = new HashMap<>(partsStats.size());

        for (StatisticsKey key : partsStats.keySet()) {
            partsStats.computeIfPresent(key, (k, v) -> {
                res.put(k, v.values());
                return v;
            });
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void replaceLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        partsStats.put(key, buildStatisticsMap(key, statistics));
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        Collection<ObjectPartitionStatisticsImpl>[] res = new Collection[1];
        partsStats.computeIfPresent(key, (k, v) -> {
            // Need to make a copy under the lock.
            res[0] = new ArrayList<>(v.values());

            return v;
        });

        return (res[0] == null) ? Collections.emptyList() : res[0];
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key) {
        partsStats.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics) {
        partsStats.compute(key, (k, v) -> {
            // Need to change the map under the lock.
            if (v == null)
                v = new IntHashMap<>();

            v.put(statistics.partId(), statistics);

            return v;
        });
    }

    /** {@inheritDoc} */
    @Override public void saveObsolescenceInfo(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence
    ) {
        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : obsolescence.entrySet()) {
            obsStats.compute(objObs.getKey(), (k, v) -> {
                if (v == null)
                    v = new IntHashMap<>();
                IntMap<ObjectPartitionStatisticsObsolescence> vFinal = v;

                objObs.getValue().forEach((k1, v1) -> vFinal.put(k1, v1));

                return v;
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void clearObsolescenceInfo(StatisticsKey key, Collection<Integer> partIds) {
        if (F.isEmpty(partIds))
            obsStats.remove(key);
        else {
            obsStats.computeIfPresent(key, (k, v) -> {
                for (Integer partId : partIds)
                    v.remove(partId);

                return v.isEmpty() ? null : v;
            });
        }
    }

    /** {@inheritDoc} */
    @Override public Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> loadAllObsolescence() {
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> res = new HashMap<>();

        obsStats.forEach((k, v) -> {
            IntHashMap newV = new IntHashMap(v.size());

            v.forEach((k1, v1) -> newV.put(k1, v1));

            res.put(k, newV);
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        ObjectPartitionStatisticsImpl[] res = new ObjectPartitionStatisticsImpl[1];
        partsStats.computeIfPresent(key, (k, v) -> {
            // Need to access the map under the lock.
            res[0] = v.get(partId);

            return v;
        });
        return res[0];
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        partsStats.computeIfPresent(key, (k, v) -> {
            v.remove(partId);

            return v;
        });
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key, Collection<Integer> partIds) {
        partsStats.computeIfPresent(key, (k, v) -> {
            for (Integer partId : partIds)
                v.remove(partId);

            return v;
        });
    }

    /**
     * Convert collection of partition level statistics into map(partId->partStatistics).
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     * @return Partition id to statistics map.
     */
    private IntMap<ObjectPartitionStatisticsImpl> buildStatisticsMap(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        IntMap<ObjectPartitionStatisticsImpl> statisticsMap = new IntHashMap<ObjectPartitionStatisticsImpl>();
        for (ObjectPartitionStatisticsImpl s : statistics) {
            if (statisticsMap.put(s.partId(), s) != null)
                log.warning(String.format("Trying to save more than one %s.%s partition statistics for partition %d",
                    key.schema(), key.obj(), s.partId()));
        }
        return statisticsMap;
    }
}
