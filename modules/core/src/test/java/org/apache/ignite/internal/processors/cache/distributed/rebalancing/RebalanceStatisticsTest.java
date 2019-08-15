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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static java.lang.Integer.parseInt;
import static java.util.Objects.nonNull;
import static java.util.function.Function.identity;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.of;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

@WithSystemProperty(key = IGNITE_QUIET, value = "false")
@WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "true")
@WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "true")
/** For testing of rebalance statistics. */
public class RebalanceStatisticsTest extends GridCommonAbstractTest {
    /** Class rule. */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** Cache names. */
    private static final String[] DEFAULT_CACHE_NAMES = {"ch0", "ch1", "ch2", "ch3"};

    /** Total information text. */
    private static final String TOTAL_INFORMATION_TEXT = "Total information";

    /** Partitions distribution text. */
    private static final String PARTITIONS_DISTRIBUTION_TEXT = "Partitions distribution per cache group";

    /** Topic statistics text. */
    public static final String TOPIC_STATISTICS_TEXT = "Topic statistics:";

    /** Supplier statistics text. */
    public static final String SUPPLIER_STATISTICS_TEXT = "Supplier statistics:";

    /** Information per cache group text. */
    public static final String INFORMATION_PER_CACHE_GROUP_TEXT = "Information per cache group";

    /** Name attribute. */
    public static final String NAME_ATTRIBUTE = "name";

    /** Multi jvm. */
    private boolean multiJvm;

    /** Node count. */
    private static final int DEFAULT_NODE_CNT = 3;

    /** Logger for listen messages. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** For remember messages from standard output. */
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(32 * 1024);

    /** For write messages from standard output. */
    private final PrintWriter pw = new PrintWriter(baos);

    /** Caches configurations. */
    private CacheConfiguration[] cacheCfgs;

    /** Coordinator. */
    private IgniteEx crd;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(cacheCfgs);
        cfg.setRebalanceThreadPoolSize(5);
        cfg.setGridLogger(log);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return multiJvm;
    }

    /**
     * Create cache configuration.
     *
     * @param cacheName Cache name.
     * @param parts Count of partitions.
     * @param backups Count backup.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(final String cacheName, final int parts, final int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));
        ccfg.setBackups(backups);
        return ccfg;
    }

    /**
     * Test check that not present statistics in log output, if we not set
     * system properties {@code IGNITE_QUIET},
     * {@code IGNITE_WRITE_REBALANCE_STATISTICS}.
     *
     * @throws Exception
     * @see IgniteSystemProperties#IGNITE_QUIET
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "true")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "false")
    public void testNotPrintStat() throws Exception {
        cacheCfgs = defaultCacheConfigurations(10, 0);

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        log.registerListener(pw::write);

        int nodeCnt = DEFAULT_NODE_CNT;

        assertNotContainsAfterCreateNewNode(nodeCnt++, TOTAL_INFORMATION_TEXT);

        System.setProperty(IGNITE_QUIET, Boolean.FALSE.toString());

        assertNotContainsAfterCreateNewNode(nodeCnt++, TOTAL_INFORMATION_TEXT);
    }

    /**
     * Test check that not present partition distribution in log output,
     * if we not set system properties
     * {@code IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS}.
     *
     * @throws Exception
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS
     */
    @Test
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "false")
    public void testNotPrintPartitionDistribution() throws Exception {
        cacheCfgs = defaultCacheConfigurations(10, 0);

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        log.registerListener(pw::write);

        assertNotContainsAfterCreateNewNode(DEFAULT_NODE_CNT, PARTITIONS_DISTRIBUTION_TEXT);
    }

    /**
     * The test checks the correctness of the output rebalance statistics.
     *
     * @throws Exception
     * */
    @Test
    public void testPrintCorrectStatistic() throws Exception {
        cacheCfgs = defaultCacheConfigurations(10,2);

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        List<String> statPerCacheGrps = new ArrayList<>();
        List<String> totalStats = new ArrayList<>();

        log.registerListener(logStr -> {
            if (!logStr.contains(INFORMATION_PER_CACHE_GROUP_TEXT))
                return;

            (logStr.contains(TOTAL_INFORMATION_TEXT) ? totalStats : statPerCacheGrps).add(logStr);
        });

        IgniteEx newNode = startGrid(DEFAULT_NODE_CNT);

        awaitPartitionMapExchange();

        //+1 - because ignite-sys-cache
        assertEquals(cacheCfgs.length + 1, statPerCacheGrps.size());
        assertEquals(1, totalStats.size());

        Map<String, Integer> partDistribution = perCacheGroupPartitionDistribution(newNode);

        Map<String, Integer> topicStats = perCacheGroupTopicStatistics(totalStats.get(0)).entrySet().stream()
            .collect(toMap(Map.Entry::getKey, entry -> sumNum(entry.getValue(), "p=([0-9]+)")));

        partDistribution.forEach((cacheName, partCnt) -> assertEquals(partCnt, topicStats.get(cacheName)));
    }

    /**
     * The test checks the correctness of the output rebalance statistics
     * in multi jvm mode.
     *
     * @throws Exception
     * */
    @Test
    public void testPrintCorrectStatisticInMultiJvm() throws Exception{
        multiJvm = true;

        cacheCfgs = defaultCacheConfigurations(100,2);

        crd = startGrids(3);

        fillCaches(100);

        Map<String, Integer> partDistribution = perCacheGroupPartitionDistribution(crd);

        stopGrid(0);

        awaitPartitionMapExchange();

        List<String> statPerCacheGrps = new ArrayList<>();
        List<String> totalStats = new ArrayList<>();

        log.registerListener(logStr -> {
            if (!logStr.contains(INFORMATION_PER_CACHE_GROUP_TEXT))
                return;

            (logStr.contains(TOTAL_INFORMATION_TEXT) ? totalStats : statPerCacheGrps).add(logStr);
        });

        IgniteEx newNode = startGrid(0);

        awaitPartitionMapExchange();

        //+1 - because ignite-sys-cache
        assertEquals(cacheCfgs.length + 1, statPerCacheGrps.size());
        assertEquals(1, totalStats.size());

        Map<String, Integer> newPartDistribution = perCacheGroupPartitionDistribution(newNode);

        Map<String, Integer> topicStats = perCacheGroupTopicStatistics(totalStats.get(0)).entrySet().stream()
            .collect(toMap(Map.Entry::getKey, entry -> sumNum(entry.getValue(), "p=([0-9]+)")));

        newPartDistribution.forEach((cacheName, partCnt) -> {
            assertEquals(partCnt, topicStats.get(cacheName));
            assertEquals(partCnt, partDistribution.get(cacheName));
        });
    }

    /**
     * Parsing and extract topic statistics string for each caches.
     *
     * @param s String with statisctics for parsing, require not null.
     * @return key - Name cache, string topic statistics.
     */
    private Map<String, String> perCacheGroupTopicStatistics(final String s) {
        assert nonNull(s);

        Map<String, String> perCacheGroupTopicStatistics = new HashMap<>();

        int startI = s.indexOf(INFORMATION_PER_CACHE_GROUP_TEXT);

        for (; ; ) {
            int tsti = s.indexOf(TOPIC_STATISTICS_TEXT, startI);
            if (tsti == -1)
                break;

            int ssti = s.indexOf(SUPPLIER_STATISTICS_TEXT, tsti);
            if (ssti == -1)
                break;

            int nai = s.indexOf(NAME_ATTRIBUTE, startI);
            if (nai == -1)
                break;

            int ci = s.indexOf(",", nai);
            if (ci == -1)
                break;

            String cacheName = s.substring(nai + NAME_ATTRIBUTE.length() + 1, ci);
            String topicStat = s.substring(tsti + TOPIC_STATISTICS_TEXT.length(), ssti);

            perCacheGroupTopicStatistics.put(cacheName, topicStat);
            startI = ssti;
        }

        return perCacheGroupTopicStatistics;
    }

    /**
     * Return partition distribution per cache groups use internal api.
     *
     * @param node Require not null.
     * @return Partition distribution per cache groups
     * */
    private Map<String, Integer> perCacheGroupPartitionDistribution(final IgniteEx node) {
        assert nonNull(node);

        ClusterNode localNode = node.localNode();

        return node.context().cache().cacheGroups().stream()
            .map(CacheGroupContext::config)
            .map(CacheConfiguration::getName)
            .collect(toMap(identity(), cacheName -> node.affinity(cacheName).allPartitions(localNode).length));
    }

    /**
     * Create {@link #DEFAULT_CACHE_NAMES} cache configurations.
     *
     * @param parts Count of partitions.
     * @param backups Count backup.
     * @return Cache group configurations.
     * */
    private CacheConfiguration[] defaultCacheConfigurations(final int parts, final int backups) {
        return of(DEFAULT_CACHE_NAMES)
            .map(cacheName -> cacheConfiguration(cacheName, parts, backups))
            .toArray(CacheConfiguration[]::new);
    }

    /**
     * Add values to all {@link #DEFAULT_CACHE_NAMES}.
     *
     * @param cnt - Count of values.
     */
    private void fillCaches(final int cnt) {
        for (CacheConfiguration cacheCfg : cacheCfgs) {
            String name = cacheCfg.getName();

            IgniteCache<Object, Object> cache = crd.cache(name);

            range(0, cnt).forEach(value -> cache.put(value, name + value));
        }
    }

    /**
     * Create new node and check that {@code notContainsStr} not present in log output.
     *
     * @param idx New node index.
     * @param notContainsStr String for assertNotContains in log output.
     * @throws Exception
     */
    private void assertNotContainsAfterCreateNewNode(final int idx, final String notContainsStr) throws Exception {
        baos.reset();

        startGrid(idx);

        awaitPartitionMapExchange();

        assertNotContains(super.log, baos.toString(), notContainsStr);
    }

    /**
     * Extract numbers and sum its.
     *
     * @param s String of numbers, require not null.
     * @param pattern Number extractor, require not null.
     * @return Sum extracted numbers.
     */
    private int sumNum(final String s, final String pattern) {
        assert nonNull(s);
        assert nonNull(pattern);

        Matcher matcher = compile(pattern).matcher(s);

        int num = 0;
        while (matcher.find())
            num += parseInt(matcher.group(1));

        return num;
    }
}
