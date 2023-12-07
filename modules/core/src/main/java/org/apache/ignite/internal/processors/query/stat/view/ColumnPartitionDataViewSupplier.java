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

package org.apache.ignite.internal.processors.query.stat.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnPartitionDataViewWalker;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsStore;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Column partition data view supplier.
 */
public class ColumnPartitionDataViewSupplier {
    /** Statistics store. */
    private final IgniteStatisticsStore store;

    /**
     * Constructor.
     *
     * @param store Statistics store.
     */
    public ColumnPartitionDataViewSupplier(IgniteStatisticsStore store) {
        this.store = store;
    }

    /**
     * Statistics column partition data view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with statistics column partition data views.
     */
    public Iterable<StatisticsColumnPartitionDataView> columnPartitionStatisticsViewSupplier(
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
}
