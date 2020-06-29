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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;

import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Utility class for rebalance statistics.
 */
public class RebalanceStatisticsUtils {
    /** Formatter for date-time objects. */
    private static final DateTimeFormatter DTF = ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    /** Supplier statistics header. */
    private static final String SUP_STAT_HEAD = "Supplier statistics: ";

    /** Supplier statistics aliases. */
    private static final String SUP_STAT_ALIASES = "Aliases: p - partitions, e - entries, b - bytes, d - duration, " +
        "h - historical, nodeId mapping (nodeId=id,consistentId) ";

    /**
     * Private constructor.
     */
    private RebalanceStatisticsUtils() {
        throw new RuntimeException("don't create");
    }

    /**
     * Returns ability to print statistics for rebalance. {@code True} if
     * {@link IgniteSystemProperties#IGNITE_QUIET IGNITE_QUIET} == {@code false}.
     *
     * @return {@code True} if printing statistics for rebalance is available.
     */
    public static boolean availablePrintRebalanceStatistics() {
        return !getBoolean(IGNITE_QUIET, true);
    }

    /**
     * Creating a string representation of group cache rebalance statistics for
     * logging.
     *
     * @param cacheGrpCtx Cache group context.
     * @param stat Rebalance statistics.
     * @param suc Flag for successful rebalancing.
     * @param top Topology version.
     * @return String representation of rebalance statistics for cache group.
     */
    public static String cacheGroupRebalanceStatistics(
        CacheGroupContext cacheGrpCtx,
        RebalanceStatistics stat,
        boolean suc,
        AffinityTopologyVersion top
    ) {
        SB sb = new SB();

        sb.a("Rebalance information per cache group (").a(suc ? "successful" : "interrupted").a(" rebalance): [")
            .a(grpInfo(cacheGrpCtx)).a(", ").a(time(stat.start(), stat.end())).a(", restarted=")
            .a(stat.attempt() - 1).a("] ");

        Map<ClusterNode, SupplierRebalanceStatistics> supStats = stat.supplierStatistics();
        if (supStats.isEmpty())
            return sb.toString();

        sb.a(SUP_STAT_HEAD);

        int nodeId = 0;
        for (SupplierRebalanceStatistics supStat : supStats.values()) {
            long fp = supStat.fullParts(), hp = supStat.histParts();
            long fe = supStat.fullEntries(), he = supStat.histEntries();
            long fb = supStat.fullBytes(), hb = supStat.histBytes();

            sb.a(supInfo(nodeId++, fp, hp, fe, he, fb, hb, supStat.start(), supStat.end()));
        }

        sb.a(SUP_STAT_ALIASES);

        nodeId = 0;
        for (ClusterNode supNode : supStats.keySet())
            sb.a(supInfo(nodeId++, supNode));

        return sb.toString();
    }

    /**
     * Creating a string representation of total rebalance statistics for all
     * cache groups for logging.
     *
     * @param totalStat Statistics of rebalance for cache groups.
     * @return String representation of total rebalance statistics
     *      for all cache groups.
     */
    public static String totalRebalanceStatistic(Map<CacheGroupContext, RebalanceStatistics> totalStat) {
        SB sb = new SB();

        long start = totalStat.values().stream().mapToLong(RebalanceStatistics::start).min().orElse(0);
        long end = totalStat.values().stream().mapToLong(RebalanceStatistics::end).max().orElse(0);

        sb.a("Rebalance total information (including successful and not rebalances): [").a(time(start, end)).a("] ");

        RebalanceStatistics total = new RebalanceStatistics();
        totalStat.values().forEach(total::merge);

        Map<ClusterNode, SupplierRebalanceStatistics> supStats = total.supplierStatistics();

        if (supStats.isEmpty())
            return sb.toString();

        sb.a(SUP_STAT_HEAD);

        int nodeId = 0;
        for (SupplierRebalanceStatistics supStat : supStats.values()) {
            long fp = supStat.fullParts(), hp = supStat.histParts();
            long fe = supStat.fullEntries(), he = supStat.histEntries();
            long fb = supStat.fullBytes(), hb = supStat.histBytes();
            long s = supStat.start(), e = supStat.end();

            sb.a(supInfo(nodeId++, fp, hp, fe, he, fb, hb, s, e));
        }

        sb.a(SUP_STAT_ALIASES);

        nodeId = 0;
        for (ClusterNode supNode : supStats.keySet())
            sb.a(supInfo(nodeId++, supNode));

        return sb.toString();
    }

    /**
     * Creation of information by supplier in format:
     * [{@code nodeId} = Consistent id].
     *
     * @param nodeId       Supplier node id.
     * @param supplierNode Supplier node.
     * @return Supplier info string.
     */
    private static String supInfo(int nodeId, ClusterNode supplierNode) {
        return new SB("[").a(nodeId).a('=').a(supplierNode.consistentId().toString()).a("] ").toString();
    }

    /**
     * Creating a rebalance statistics string for supplier.
     *
     * @param nodeId Supplier node id.
     * @param fp Counter of partitions received by full rebalance.
     * @param hp Counter of partitions received by historical rebalance.
     * @param fe Counter of entries received by full rebalance.
     * @param he Counter of entries received by historical rebalance.
     * @param fb Counter of bytes received by full rebalance.
     * @param hb Counter of bytes received by historical rebalance.
     * @param s Start time of rebalance in milliseconds.
     * @param e End time of rebalance in milliseconds.
     * @return Supplier info string.
     */
    private static String supInfo(int nodeId, long fp, long hp, long fe, long he, long fb, long hb, long s, long e) {
        SB sb = new SB();
        sb.a("[nodeId=").a(nodeId);

        if (fp > 0)
            sb.a(", p=").a(fp);

        if (hp > 0)
            sb.a(", hp=").a(hp);

        if (fe > 0)
            sb.a(", e=").a(fe);

        if (he > 0)
            sb.a(", he=").a(he);

        if (fb > 0)
            sb.a(", b=").a(fb);

        if (hb > 0)
            sb.a(", hb=").a(hb);

        return sb.a(", d=").a(e - s).a(" ms] ").toString();
    }

    /**
     * Creating a string with time information.
     *
     * @param start Start time in milliseconds.
     * @param end   End time in milliseconds.
     * @return String with time information.
     */
    private static String time(long start, long end) {
        return new SB().a("startTime=").a(DTF.format(toLocalDateTime(start))).a(", finishTime=")
            .a(DTF.format(toLocalDateTime(end))).a(", d=").a(end - start).a(" ms").toString();
    }

    /**
     * Creating information for a cache group.
     *
     * @param cacheGrpCtx Cache group context.
     * @return Group info.
     */
    private static String grpInfo(CacheGroupContext cacheGrpCtx) {
        return new SB().a("id=").a(cacheGrpCtx.groupId()).a(", name=").a(cacheGrpCtx.cacheOrGroupName()).toString();
    }

    /**
     * Convert time in millis to local date time.
     *
     * @param time Time in mills.
     * @return The local date-time.
     */
    private static LocalDateTime toLocalDateTime(final long time) {
        return new Date(time).toInstant().atZone(systemDefault()).toLocalDateTime();
    }
}
