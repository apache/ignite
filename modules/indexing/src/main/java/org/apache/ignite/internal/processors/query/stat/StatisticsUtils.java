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
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.value.Value;

/**
 * Utilities to convert statistics from/to messages.
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
        GridH2ValueMessage msgMin = stat.min() == null ? null : GridH2ValueMessageFactory.toMessage(stat.min());
        GridH2ValueMessage msgMax = stat.max() == null ? null : GridH2ValueMessageFactory.toMessage(stat.max());

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
        Value min = (data.min() == null) ? null : data.min().value(ctx);
        Value max = (data.max() == null) ? null : data.max().value(ctx);

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
            ObjectPartitionStatisticsImpl partStats = (ObjectPartitionStatisticsImpl) stat;

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
}
