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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsDecimalMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlDate;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlTime;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToTimestamp;

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
        StatisticsDecimalMessage msgMin = new StatisticsDecimalMessage(stat.min());
        StatisticsDecimalMessage msgMax = new StatisticsDecimalMessage(stat.max());

        return new StatisticsColumnData(msgMin, msgMax, stat.nulls(), stat.distinct(),
            stat.total(), stat.size(), stat.raw(), stat.version(), stat.createdAt());
    }

    /**
     * Convert statistics column data message to column statistics object.
     *
     * @param ctx Kernal context.
     * @param data Statistics column data message to convert.
     * @return ColumnStatistics object.
     */
    public static ColumnStatistics toColumnStatistics(GridKernalContext ctx, StatisticsColumnData data) {
        return new ColumnStatistics(data.min().value(), data.max().value(), data.nulls(), data.distinct(),
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
     */
    public static ObjectStatisticsImpl toObjectStatistics(GridKernalContext ctx, StatisticsObjectData data) {
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

        for (Map.Entry<String, Long> ver : versions.entrySet()) {
            ColumnStatistics colStat = stat.columnsStatistics().get(ver.getKey());

            if (colStat == null || colStat.version() < ver.getValue())
                return -1;

            if (colStat.version() > ver.getValue())
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

        for (Map.Entry<String, Long> colVer : versions.entrySet()) {
            StatisticsColumnConfiguration colCfg = cfg.columns().get(colVer.getKey());

            if (colCfg == null || colCfg.version() < colVer.getValue())
                return -1;

            if (colCfg.version() > colVer.getValue())
                return 1;
        }

        return 0;
    }

    /** */
    public static BigDecimal toDecimal(Object obj) {
        if (obj == null)
            return null;

        Class<?> cls = U.box(obj.getClass());

        if (Boolean.class.isAssignableFrom(cls))
            return (Boolean)obj ? BigDecimal.ONE : BigDecimal.ZERO;
        else if (Byte.class.isAssignableFrom(cls))
            return BigDecimal.valueOf((Byte)obj);
        else if (Short.class.isAssignableFrom(cls))
            return BigDecimal.valueOf((Short)obj);
        else if (Integer.class.isAssignableFrom(cls))
            return BigDecimal.valueOf((Integer)obj);
        else if (Long.class.isAssignableFrom(cls))
            return new BigDecimal((Long)obj);
        else if (Float.class.isAssignableFrom(cls))
            return BigDecimal.valueOf((Float)obj);
        else if (Double.class.isAssignableFrom(cls))
            return BigDecimal.valueOf((Double)obj);
        else if (BigDecimal.class.isAssignableFrom(cls))
            return (BigDecimal)obj;
        else if (UUID.class.isAssignableFrom(cls)) {
            BigInteger bigInt = new BigInteger(1, U.uuidToBytes((UUID)obj));
            return new BigDecimal(bigInt);
        }
        else if (java.util.Date.class.isAssignableFrom(cls))
            return new BigDecimal(((java.util.Date)obj).getTime());
        else if (LocalDate.class.isAssignableFrom(cls))
            return new BigDecimal(convertToSqlDate((LocalDate)obj).getTime());
        else if (LocalTime.class.isAssignableFrom(cls))
            return new BigDecimal(convertToSqlTime((LocalTime)obj).getTime());
        else if (LocalDateTime.class.isAssignableFrom(cls))
            return new BigDecimal(convertToTimestamp((LocalDateTime)obj).getTime());

        throw new IllegalArgumentException("Value of type " + cls.getName() + " is not expected");
    }
}
