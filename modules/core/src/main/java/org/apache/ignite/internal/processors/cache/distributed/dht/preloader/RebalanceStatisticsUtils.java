/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.nonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collector.of;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Utility class for rebalance statistics.
 */
class RebalanceStatisticsUtils {
    /** To format the date and time. */
    private static final DateTimeFormatter REBALANCE_STATISTICS_DTF = ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    /** Text for successful or not rebalances. */
    private static final String SUCCESSFUL_OR_NOT_REBALANCE_TEXT = "including successful and not rebalances";

    /** Text successful rebalance. */
    private static final String SUCCESSFUL_REBALANCE_TEXT = "successful rebalance";

    /**
     * Private constructor.
     */
    private RebalanceStatisticsUtils() {
        throw new RuntimeException("don't create");
    }

    /** Rebalance future statistics. */
    static class RebalanceFutureStatistics {
        /** Start rebalance time in mills. */
        private final long startTime = currentTimeMillis();

        /** End rebalance time in mills. */
        private volatile long endTime = startTime;

        /** First key - topic id. Second key - supplier node. */
        private final Map<Integer, Map<ClusterNode, RebalanceMessageStatistics>> msgStats = new ConcurrentHashMap<>();

        /** Is needed or not to print rebalance statistics. */
        private final boolean printRebalanceStatistics = printRebalanceStatistics();

        /**
         * Add new message statistics.
         * Requires to be invoked before demand message sending.
         * This method required for {@code addReceivePartitionStatistics}.
         * This method add new message statistics if
         * {@link #printRebalanceStatistics} == true.
         *
         * @param topicId Topic id, require not null.
         * @param supplierNode Supplier node, require not null.
         * @see RebalanceMessageStatistics
         * @see #addReceivePartitionStatistics(Integer, ClusterNode, GridDhtPartitionSupplyMessage)
         */
        public void addMessageStatistics(final Integer topicId, final ClusterNode supplierNode) {
            assert nonNull(topicId);
            assert nonNull(supplierNode);

            if (!printRebalanceStatistics)
                return;

            msgStats.computeIfAbsent(topicId, integer -> new ConcurrentHashMap<>())
                .put(supplierNode, new RebalanceMessageStatistics(currentTimeMillis()));
        }

        /**
         * Add new statistics by receive message with partitions from supplier
         * node. Require invoke {@code addMessageStatistics} before send
         * demand message. This method add new message statistics if
         * {@link #printRebalanceStatistics} == true.
         *
         * @param topicId Topic id, require not null.
         * @param supplierNode Supplier node, require not null.
         * @param supplyMsg Supply message, require not null.
         * @see ReceivePartitionStatistics
         * @see #addMessageStatistics(Integer, ClusterNode)
         */
        public void addReceivePartitionStatistics(
            final Integer topicId,
            final ClusterNode supplierNode,
            final GridDhtPartitionSupplyMessage supplyMsg
        ) {
            assert nonNull(topicId);
            assert nonNull(supplierNode);
            assert nonNull(supplyMsg);

            if (!printRebalanceStatistics)
                return;

            List<PartitionStatistics> partStats = supplyMsg.infos().entrySet().stream()
                .map(entry -> new PartitionStatistics(entry.getKey(), entry.getValue().infos().size()))
                .collect(toList());

            msgStats.get(topicId).get(supplierNode).receivePartStats
                .add(new ReceivePartitionStatistics(currentTimeMillis(), supplyMsg.messageSize(), partStats));
        }

        /**
         * Clear statistics.
         */
        public void clear() {
            msgStats.clear();
        }

        /**
         * Set end rebalance time in mills.
         *
         * @param endTime End rebalance time in mills.
         */
        public void endTime(final long endTime) {
            this.endTime = endTime;
        }
    }

    /** Rebalance messages statistics. */
    static class RebalanceMessageStatistics {
        /** Time send demand message in mills. */
        private final long sndMsgTime;

        /** Statistics by received partitions. */
        private final Collection<ReceivePartitionStatistics> receivePartStats = new ConcurrentLinkedQueue<>();

        /**
         * Constructor.
         *
         * @param sndMsgTime time send demand message.
         */
        public RebalanceMessageStatistics(final long sndMsgTime) {
            this.sndMsgTime = sndMsgTime;
        }
    }

    /** Receive partition statistics. */
    static class ReceivePartitionStatistics {
        /** Time receive message(on demand message) with partition in mills. */
        private final long rcvMsgTime;

        /** Size receive message in bytes. */
        private final long msgSize;

        /** Received partitions. */
        private final List<PartitionStatistics> parts;

        /**
         * Constructor.
         *
         * @param rcvMsgTime time receive message in mills.
         * @param msgSize message size in bytes.
         * @param parts received partitions, require not null.
         */
        public ReceivePartitionStatistics(
            final long rcvMsgTime,
            final long msgSize,
            final List<PartitionStatistics> parts
        ) {
            assert nonNull(parts);

            this.rcvMsgTime = rcvMsgTime;
            this.msgSize = msgSize;
            this.parts = parts;
        }
    }

    /** Received partition info. */
    static class PartitionStatistics {
        /** Partition id. */
        private final int id;

        /** Count entries in partition. */
        private final int entryCount;

        /**
         * Constructor.
         *
         * @param id partition id.
         * @param entryCount count entries in partitions.
         */
        public PartitionStatistics(final int id, final int entryCount) {
            this.id = id;
            this.entryCount = entryCount;
        }
    }

    /**
     * Finds out if statistics can be printed regarding
     * {@link IgniteSystemProperties#IGNITE_QUIET},
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS}.
     *
     * @return Is print statistics enabled.
     */
    public static boolean printRebalanceStatistics() {
        return !getBoolean(IGNITE_QUIET, true) && getBoolean(IGNITE_WRITE_REBALANCE_STATISTICS, false);
    }

    /**
     * Finds out if partitions distribution can be printed regarding
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS}.
     *
     * @return Is print partitions distribution enabled.
     */
    public static boolean printPartitionsDistribution() {
        return getBoolean(IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, false);
    }

    /**
     * Return rebalance statistics. Required to call this method if
     * {@link #printRebalanceStatistics()} == true.
     * <p/>
     * Flag {@code finish} should reflect was full rebalance finished or not.
     * <br/>
     * If {@code finish} == true then expected {@code rebFutrs} contains
     * successful or not {@code RebalanceFuture} per cache group, else expected
     * {@code rebFutrs} contains only one successful {@code RebalanceFuture}.
     * <br/>
     * If {@code finish} == true then print total statistics.
     * <p/>
     * Partition distribution is printed only for last success rebalance,
     * per cache group.
     *
     * @param finish Is the whole rebalance finished or not.
     * @param rebFutrs Involved in rebalance, require not null.
     * @return String with printed rebalance statistics.
     * @throws IgniteCheckedException Could be thrown while getting result of
     *      {@code RebalanceFuture}.
     * @see RebalanceFuture RebalanceFuture
     */
    public static String rebalanceStatistics(
        final boolean finish,
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs
    ) throws IgniteCheckedException {
        assert nonNull(rebFutrs);
        assert printRebalanceStatistics() : "Can't print statistics";

        AtomicInteger nodeCnt = new AtomicInteger();

        Map<ClusterNode, Integer> nodeAliases = toRebalanceFutureStream(rebFutrs)
            .flatMap(future -> future.stat.msgStats.entrySet().stream())
            .flatMap(entry -> entry.getValue().keySet().stream())
            .distinct()
            .collect(toMap(identity(), node -> nodeCnt.getAndIncrement()));

        StringJoiner joiner = new StringJoiner(" ");

        if (finish)
            writeTotalRebalanceStatistics(rebFutrs, nodeAliases, joiner);

        writeCacheGroupsRebalanceStatistics(rebFutrs, nodeAliases, finish, joiner);
        writeAliasesRebalanceStatistics("p - partitions, e - entries, b - bytes, d - duration", nodeAliases, joiner);
        writePartitionsDistributionRebalanceStatistics(rebFutrs, nodeAliases, nodeCnt, joiner);

        return joiner.toString();
    }

    /**
     * Write total statistics for rebalance.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeTotalRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        long minStartTime = minStartTime(toRebalanceFutureStream(rebFutrs));
        long maxEndTime = maxEndTime(toRebalanceFutureStream(rebFutrs));

        joiner.add("Total information (" + SUCCESSFUL_OR_NOT_REBALANCE_TEXT + "):")
            .add("Time").add("[" + toStartEndDuration(minStartTime, maxEndTime) + "]");

        Map<Integer, List<RebalanceMessageStatistics>> topicStat =
            toTopicStatistics(toRebalanceFutureStream(rebFutrs));
        writeTopicRebalanceStatistics(topicStat, joiner);

        Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat =
            toSupplierStatistics(toRebalanceFutureStream(rebFutrs));
        writeSupplierRebalanceStatistics(supplierStat, nodeAliases, joiner);
    }

    /**
     * Write rebalance statistics per cache group.
     * <p/>
     * If {@code finish} == true then add {@link #SUCCESSFUL_OR_NOT_REBALANCE_TEXT} else add {@link
     * #SUCCESSFUL_REBALANCE_TEXT} into header.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     * @param finish Is finish rebalance.
     */
    private static void writeCacheGroupsRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final boolean finish,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        joiner.add("Information per cache group (" +
            (finish ? SUCCESSFUL_OR_NOT_REBALANCE_TEXT : SUCCESSFUL_REBALANCE_TEXT) + "):");

        rebFutrs.forEach((context, futures) -> {
            long minStartTime = minStartTime(futures.stream());
            long maxEndTime = maxEndTime(futures.stream());

            joiner.add("[id=" + context.groupId() + ",")
                .add("name=" + context.cacheOrGroupName() + ",")
                .add(toStartEndDuration(minStartTime, maxEndTime) + "]");

            Map<Integer, List<RebalanceMessageStatistics>> topicStat = toTopicStatistics(futures.stream());
            writeTopicRebalanceStatistics(topicStat, joiner);

            Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat = toSupplierStatistics(futures.stream());
            writeSupplierRebalanceStatistics(supplierStat, nodeAliases, joiner);
        });
    }

    /**
     * Write partitions distribution per cache group. Only for last success rebalance.
     * Works if {@link #printPartitionsDistribution()} return true.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param nodeCnt For adding new nodes into {@code nodeAliases}, require not null.
     * @param joiner For write statistics, require not null.
     * @throws IgniteCheckedException When get result of
     *      {@link RebalanceFuture}.
     */
    private static void writePartitionsDistributionRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final AtomicInteger nodeCnt,
        final StringJoiner joiner
    ) throws IgniteCheckedException {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(nodeCnt);
        assert nonNull(joiner);

        if (!printPartitionsDistribution())
            return;

        joiner.add("Partitions distribution per cache group (" + SUCCESSFUL_REBALANCE_TEXT + "):");

        Comparator<RebalanceFuture> startTimeCmp = comparingLong(fut -> fut.stat.startTime);
        Comparator<RebalanceFuture> startTimeCmpReversed = startTimeCmp.reversed();

        Comparator<PartitionStatistics> partIdCmp = comparingInt(value -> value.id);
        Comparator<ClusterNode> nodeAliasesCmp = comparingInt(nodeAliases::get);

        for (Entry<CacheGroupContext, Collection<RebalanceFuture>> rebFutrsEntry : rebFutrs.entrySet()) {
            CacheGroupContext cacheGrpCtx = rebFutrsEntry.getKey();

            joiner.add("[id=" + cacheGrpCtx.groupId() + ",")
                .add("name=" + cacheGrpCtx.cacheOrGroupName() + "]");

            List<RebalanceFuture> successFutures = new ArrayList<>();

            for (RebalanceFuture rebalanceFuture : rebFutrsEntry.getValue()) {
                if (rebalanceFuture.isDone() && rebalanceFuture.get())
                    successFutures.add(rebalanceFuture);
            }

            if (successFutures.isEmpty())
                return;

            successFutures.sort(startTimeCmpReversed);

            RebalanceFuture lastSuccessFuture = successFutures.get(0);

            AffinityAssignment affinity = cacheGrpCtx.affinity().cachedAffinity(lastSuccessFuture.topologyVersion());

            Map<PartitionStatistics, ClusterNode> supplierNodeRcvParts = new TreeMap<>(partIdCmp);

            for (Entry<Integer, Map<ClusterNode, RebalanceMessageStatistics>> topicStatEntry : lastSuccessFuture.stat
                .msgStats.entrySet()) {
                for (Entry<ClusterNode, RebalanceMessageStatistics> supplierStatEntry : topicStatEntry.getValue().entrySet()) {
                    for (ReceivePartitionStatistics receivePartStat : supplierStatEntry.getValue().receivePartStats) {
                        for (PartitionStatistics partStat : receivePartStat.parts)
                            supplierNodeRcvParts.put(partStat, supplierStatEntry.getKey());
                    }
                }
            }

            affinity.nodes().forEach(node -> nodeAliases.computeIfAbsent(node, node1 -> nodeCnt.getAndIncrement()));

            for (Entry<PartitionStatistics, ClusterNode> supplierNodeRcvPart : supplierNodeRcvParts.entrySet()) {
                int partId = supplierNodeRcvPart.getKey().id;

                String nodes = affinity.get(partId).stream()
                    .sorted(nodeAliasesCmp)
                    .map(node -> "[" + nodeAliases.get(node) +
                        (affinity.primaryPartitions(node.id()).contains(partId) ? ",pr" : ",bu") +
                        (node.equals(supplierNodeRcvPart.getValue()) ? ",su" : "") + "]"
                    )
                    .collect(joining(","));

                joiner.add(valueOf(partId)).add("=").add(nodes);
            }
        }

        writeAliasesRebalanceStatistics("pr - primary, bu - backup, su - supplier node", nodeAliases, joiner);
    }

    /**
     * Write statistics per topic.
     *
     * @param topicStat Statistics by topics (in successful and not rebalances), require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeTopicRebalanceStatistics(
        final Map<Integer, List<RebalanceMessageStatistics>> topicStat,
        final StringJoiner joiner
    ) {
        assert nonNull(topicStat);
        assert nonNull(joiner);

        joiner.add("Topic statistics:");

        topicStat.forEach((topicId, msgStats) -> {
            long partCnt = sum(msgStats, rps -> rps.parts.size());
            long byteSum = sum(msgStats, rps -> rps.msgSize);
            long entryCount = sum(msgStats, rps -> rps.parts.stream().mapToLong(ps -> ps.entryCount).sum());

            joiner.add("[id=" + topicId + ",")
                .add(toPartitionsEntriesBytes(partCnt, entryCount, byteSum) + "]");
        });
    }

    /**
     * Write stattistics per supplier node.
     *
     * @param supplierStat Statistics by supplier (in successful and not rebalances), require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeSupplierRebalanceStatistics(
        final Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(supplierStat);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        joiner.add("Supplier statistics:");

        supplierStat.forEach((supplierNode, msgStats) -> {
            long partCnt = sum(msgStats, rps -> rps.parts.size());
            long byteSum = sum(msgStats, rps -> rps.msgSize);
            long entryCount = sum(msgStats, rps -> rps.parts.stream().mapToLong(ps -> ps.entryCount).sum());

            long durationSum = msgStats.stream()
                .flatMapToLong(msgStat -> msgStat.receivePartStats.stream()
                    .mapToLong(rps -> rps.rcvMsgTime - msgStat.sndMsgTime)
                )
                .sum();

            joiner.add("[nodeId=" + nodeAliases.get(supplierNode) + ",")
                .add(toPartitionsEntriesBytes(partCnt, entryCount, byteSum) + ",")
                .add("d=" + durationSum + " ms]");
        });
    }

    /**
     * Write statistics aliases, for reducing output string.
     *
     * @param nodeAliases for print nodeId=1 instead long string, require not null.
     * @param abbreviations Abbreviations ex. b - bytes, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void writeAliasesRebalanceStatistics(
        final String abbreviations,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(abbreviations);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        String nodes = nodeAliases.entrySet().stream()
            .sorted(comparingInt(Entry::getValue))
            .map(entry -> "[" + entry.getValue() + "=" + entry.getKey().id() + "," + entry.getKey().consistentId() + "]")
            .collect(joining(", "));

        joiner.add("Aliases:").add(abbreviations + ",").add("nodeId mapping (nodeId=id,consistentId)").add(nodes);
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

    /**
     * Get min {@link RebalanceFutureStatistics#startTime} in stream rebalance future's.
     *
     * @param stream Stream rebalance future, require not null.
     * @return Min start time.
     */
    private static long minStartTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.startTime).min().orElse(0);
    }

    /**
     * Get max {@link RebalanceFutureStatistics#endTime} in stream rebalance future's.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Max end time.
     */
    private static long maxEndTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.endTime).max().orElse(0);
    }

    /**
     * Prepare stream rebalance future's of each cache groups.
     *
     * @param rebFutrs Rebalance future's by cache groups, require not null.
     * @return Stream rebalance future's.
     */
    private static Stream<RebalanceFuture> toRebalanceFutureStream(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs
    ) {
        assert nonNull(rebFutrs);

        return rebFutrs.entrySet().stream().flatMap(entry -> entry.getValue().stream());
    }

    /**
     * Aggregates statistics by topic number.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Statistic by topics.
     */
    private static Map<Integer, List<RebalanceMessageStatistics>> toTopicStatistics(
        final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.flatMap(future -> future.stat.msgStats.entrySet().stream())
            .collect(groupingBy(
                Entry::getKey,
                mapping(
                    entry -> entry.getValue().values(),
                    of(
                        ArrayList::new,
                        Collection::addAll,
                        (ms1, ms2) -> {
                            ms1.addAll(ms2);
                            return ms1;
                        }
                    )
                )
            ));
    }

    /**
     * Aggregates statistics by supplier node.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Statistic by supplier.
     */
    private static Map<ClusterNode, List<RebalanceMessageStatistics>> toSupplierStatistics(
        final Stream<RebalanceFuture> stream
    ) {
        assert nonNull(stream);

        return stream.flatMap(future -> future.stat.msgStats.entrySet().stream())
            .flatMap(entry -> entry.getValue().entrySet().stream())
            .collect(groupingBy(Entry::getKey, mapping(Entry::getValue, toList())));
    }

    /**
     * Creates a string containing the beginning, end, and duration of the rebalance.
     *
     * @param start Start time in ms.
     * @param end End time in ms.
     * @return Formatted string of rebalance time.
     * @see #REBALANCE_STATISTICS_DTF
     */
    private static String toStartEndDuration(final long start, final long end) {
        return "start=" + REBALANCE_STATISTICS_DTF.format(toLocalDateTime(start)) + ", end=" +
            REBALANCE_STATISTICS_DTF.format(toLocalDateTime(end)) + ", d=" + (end - start) + " ms";
    }

    /**
     * Summarizes long values.
     *
     * @param msgStats Message statistics, require not null.
     * @param longExtractor Long extractor, require not null.
     * @return Sum of long values.
     */
    private static long sum(
        final List<RebalanceMessageStatistics> msgStats,
        final ToLongFunction<? super ReceivePartitionStatistics> longExtractor
    ) {
        assert nonNull(msgStats);
        assert nonNull(longExtractor);

        return msgStats.stream()
            .flatMap(msgStat -> msgStat.receivePartStats.stream())
            .mapToLong(longExtractor)
            .sum();
    }

    /**
     * Create a string containing count received partitions,
     * count received entries and sum received bytes.
     *
     * @param parts Count received partitions.
     * @param entries Count received entries.
     * @param bytes Sum received bytes.
     * @return Formatted string of received rebalance partitions.
     */
    private static String toPartitionsEntriesBytes(final long parts, final long entries, final long bytes) {
        return "p=" + parts + ", e=" + entries + ", b=" + bytes;
    }
}
