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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnLocalDataViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnPartitionDataViewWalker;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Column local data view supplier.
 */
public class ColumnLocalDataViewSupplier {
    /** Statistics repository. */
    private final IgniteStatisticsRepository repo;

    /**
     * Constructor.
     *
     * @param repository Statistics repository.
     */
    public ColumnLocalDataViewSupplier(IgniteStatisticsRepository repository) {
        repo = repository;
    }

    /**
     * Statistics column local node data view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with statistics column local node data views.
     */
    public Iterable<StatisticsColumnLocalDataView> columnLocalStatisticsViewSupplier(Map<String, Object> filter) {
        String type = (String)filter.get(StatisticsColumnPartitionDataViewWalker.TYPE_FILTER);

        if (type != null && !StatisticsColumnConfigurationView.TABLE_TYPE.equalsIgnoreCase(type))
            return Collections.emptyList();

        String schema = (String)filter.get(StatisticsColumnLocalDataViewWalker.SCHEMA_FILTER);
        String name = (String)filter.get(StatisticsColumnLocalDataViewWalker.NAME_FILTER);
        String column = (String)filter.get(StatisticsColumnPartitionDataViewWalker.COLUMN_FILTER);

        Map<StatisticsKey, ObjectStatisticsImpl> locStatsMap;

        if (!F.isEmpty(schema) && !F.isEmpty(name)) {
            StatisticsKey key = new StatisticsKey(schema, name);

            ObjectStatisticsImpl objLocStat = repo.getLocalStatistics(key, null);

            if (objLocStat == null)
                return Collections.emptyList();

            locStatsMap = Collections.singletonMap(key, objLocStat);
        }
        else
            locStatsMap = repo.localStatisticsMap().entrySet().stream()
                .filter(e -> F.isEmpty(schema) || schema.equals(e.getKey().schema()))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().statistics()));

        List<StatisticsColumnLocalDataView> res = new ArrayList<>();

        for (Map.Entry<StatisticsKey, ObjectStatisticsImpl> locStatsEntry : locStatsMap.entrySet()) {
            StatisticsKey key = locStatsEntry.getKey();
            ObjectStatisticsImpl stat = locStatsEntry.getValue();

            if (column == null) {
                for (Map.Entry<String, ColumnStatistics> colStat : locStatsEntry.getValue().columnsStatistics()
                    .entrySet()) {
                    StatisticsColumnLocalDataView colStatView = new StatisticsColumnLocalDataView(key, colStat.getKey(),
                        stat);

                    res.add(colStatView);
                }
            }
            else {
                ColumnStatistics colStat = locStatsEntry.getValue().columnStatistics(column);

                if (colStat != null) {
                    StatisticsColumnLocalDataView colStatView = new StatisticsColumnLocalDataView(key, column, stat);

                    res.add(colStatView);
                }
            }
        }

        return res;
    }
}
