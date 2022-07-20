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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsBasicValueMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utilities to convert statistics from/to messages, validate configurations with statistics and so on.
 */
public class StatisticsUtils {
    /**
     * Convert ColumnStatistics to StaticColumnData message.
     *
     * @param stat Column statistics to convert.
     * @return Converted stats column data message.
     * @throws IgniteCheckedException In case of errors.
     */
    public static StatisticsColumnData toMessage(ColumnStatistics stat) throws IgniteCheckedException {
        StatisticsBasicValueMessage msgMin = stat.min() == null ? null : new StatisticsBasicValueMessage(stat.min());
        StatisticsBasicValueMessage msgMax = stat.max() == null ? null : new StatisticsBasicValueMessage(stat.max());

        return new StatisticsColumnData(msgMin, msgMax, stat.nulls(), stat.distinct(),
            stat.total(), stat.size(), stat.raw(), stat.version(), stat.createdAt());
    }

    /**
     * Convert statistics column data message to column statistics object.
     *
     * @param ctx Kernal context.
     * @param data Statistics column data message to convert.
     * @return ColumnStatistics object.
     * @throws IgniteCheckedException In case of errors.
     */
    public static ColumnStatistics toColumnStatistics(
        GridKernalContext ctx,
        StatisticsColumnData data
    ) throws IgniteCheckedException {
        IndexKey min = (data.min() == null) ? null : data.min().value(ctx);
        IndexKey max = (data.max() == null) ? null : data.max().value(ctx);

        return new ColumnStatistics(min, max, data.nulls(), data.distinct(),
            data.total(), data.size(), data.rawData(), data.version(), data.createdAt());
    }

    /**
     * Build statistics object data from values.
     *
     * @param keyMsg Statistics key.
     * @param type Statistics type.
     * @param stat Object statistics to convert.
     * @return Converted StatsObjectData message.
     * @throws IgniteCheckedException In case of errors.
     */
    public static StatisticsObjectData toObjectData(
        StatisticsKeyMessage keyMsg,
        StatisticsType type,
        ObjectStatisticsImpl stat
    ) throws IgniteCheckedException {
        Map<String, StatisticsColumnData> colData = new HashMap<>(stat.columnsStatistics().size());

        for (Map.Entry<String, ColumnStatistics> ts : stat.columnsStatistics().entrySet())
            colData.put(ts.getKey(), toMessage(ts.getValue()));

        StatisticsObjectData data;

        if (stat instanceof ObjectPartitionStatisticsImpl) {
            ObjectPartitionStatisticsImpl partStats = (ObjectPartitionStatisticsImpl)stat;

            data = new StatisticsObjectData(keyMsg, stat.rowCount(), type, partStats.partId(),
                partStats.updCnt(), colData);
        }
        else
            data = new StatisticsObjectData(keyMsg, stat.rowCount(), type, 0, 0, colData);
        return data;
    }

    /**
     * Build stats key message.
     *
     * @param schema Schema name.
     * @param obj Object name.
     * @param colNames Column names or {@code null}.
     * @return Statistics key message.
     */
    public static StatisticsKeyMessage toMessage(String schema, String obj, String... colNames) {
        return new StatisticsKeyMessage(schema, obj, F.asList(colNames));
    }

    /**
     * Convert StatsObjectData message to ObjectPartitionStatistics.
     *
     * @param ctx Kernal context to use during conversion.
     * @param objData StatsObjectData to convert.
     * @return Converted ObjectPartitionStatistics.
     * @throws IgniteCheckedException In case of errors.
     */
    public static ObjectPartitionStatisticsImpl toObjectPartitionStatistics(
        GridKernalContext ctx,
        StatisticsObjectData objData
    ) throws IgniteCheckedException {
        if (objData == null)
            return null;

        assert objData.type() == StatisticsType.PARTITION;

        Map<String, ColumnStatistics> colNameToStat = new HashMap<>(objData.data().size());

        for (Map.Entry<String, StatisticsColumnData> cs : objData.data().entrySet())
            colNameToStat.put(cs.getKey(), toColumnStatistics(ctx, cs.getValue()));

        return new ObjectPartitionStatisticsImpl(
            objData.partId(),
            objData.rowsCnt(),
            objData.updCnt(),
            colNameToStat
        );
    }

    /**
     * Convert statistics object data message to object statistics impl.
     *
     * @param ctx Kernal context to use during conversion.
     * @param data Statistics object data message to convert.
     * @return Converted object statistics.
     * @throws IgniteCheckedException  In case of errors.
     */
    public static ObjectStatisticsImpl toObjectStatistics(
        GridKernalContext ctx,
        StatisticsObjectData data
    ) throws IgniteCheckedException {
        Map<String, ColumnStatistics> colNameToStat = new HashMap<>(data.data().size());

        for (Map.Entry<String, StatisticsColumnData> cs : data.data().entrySet())
            colNameToStat.put(cs.getKey(), toColumnStatistics(ctx, cs.getValue()));

        return new ObjectStatisticsImpl(data.rowsCnt(), colNameToStat);
    }

    /**
     * Create statistics target from statistics key message.
     *
     * @param msg Source statistics key message;
     * @return StatisticsTarget.
     */
    public static StatisticsTarget statisticsTarget(StatisticsKeyMessage msg) {
        String[] cols = (msg.colNames() == null) ? null : msg.colNames().toArray(new String[0]);
        return new StatisticsTarget(msg.schema(), msg.obj(), cols);
    }

    /**
     * Test if specified statistics are fit to all required versions.
     * It means that specified statistics contains all columns with at least versions
     * from specified map.
     *
     * @param stat Statistics to check. Can be {@code null}.
     * @param versions Map of column name to required version.
     * @return positive value if statistics versions bigger than specified in versions map,
     *         negative value if statistics version smaller than specified in version map,
     *         zero it they are equals.
     */
    public static int compareVersions(
        ObjectStatisticsImpl stat,
        Map<String, Long> versions
    ) {
        if (stat == null)
            return -1;

        for (Map.Entry<String, Long> version : versions.entrySet()) {
            ColumnStatistics colStat = stat.columnsStatistics().get(version.getKey());

            if (colStat == null || colStat.version() < version.getValue())
                return -1;

            if (colStat.version() > version.getValue())
                return 1;
        }

        return 0;
    }

    /**
     * Test if secified statistics configuration is fit to all required versions.
     * It means that specified statistics configuraion contains all columns with at least versions from specified map.
     *
     * @param cfg Statistics configuraion to check. Can be {@code null}.
     * @param versions Map of column name to required version.
     * @return {@code true} if it is, {@code talse} otherwise.
     */
    public static int compareVersions(
        StatisticsObjectConfiguration cfg,
        Map<String, Long> versions
    ) {
        if (cfg == null)
            return -1;

        for (Map.Entry<String, Long> colVersion : versions.entrySet()) {
            StatisticsColumnConfiguration colCfg = cfg.columns().get(colVersion.getKey());

            if (colCfg == null || colCfg.version() < colVersion.getValue())
                return -1;

            if (colCfg.version() > colVersion.getValue())
                return 1;
        }

        return 0;
    }
}
