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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.of;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Class checks the presence of evicted partitions in log.
 */
@WithSystemProperty(key = "SHOW_EVICTION_PROGRESS_FREQ", value = "10")
public class EvictPartitionInLogTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Cache names. */
    private static final String[] DEFAULT_CACHE_NAMES = {DEFAULT_CACHE_NAME + "0", DEFAULT_CACHE_NAME + "1"};

    /** Cache's backups. */
    public int backups = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        clearStaticLog(GridDhtLocalPartition.class);

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        testLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog)
            .setCacheConfiguration(
                of(DEFAULT_CACHE_NAMES)
                    .map(cacheName ->
                        new CacheConfiguration<>(cacheName)
                            .setGroupName(cacheName)
                            .setBackups(backups)
                            .setAffinity(new RendezvousAffinityFunction(false, 12))
                            .setIndexedTypes(Integer.class, Integer.class)
                    ).toArray(CacheConfiguration[]::new)
            );
    }

    /**
     * Test checks the presence of evicted partitions (RENTING state) in log without duplicate partitions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictPartByRentingState() throws Exception {
        IgniteEx node = startGrid(0);
        awaitPartitionMapExchange();

        Map<Integer, Collection<Integer>> parseParts = new ConcurrentHashMap<>();

        LogListener logLsnr = logListener("eviction", parseParts, DEFAULT_CACHE_NAMES);
        testLog.registerListener(logLsnr);

        List<GridDhtLocalPartition> parts = of(DEFAULT_CACHE_NAMES)
            .map(node::cache)
            .map(GridCommonAbstractTest::internalCache0)
            .flatMap(internalCache -> internalCache.context().topology().localPartitions().stream())
            .peek(p -> p.setState(RENTING))
            .collect(toList());

        parts.subList(0, parts.size() - 1).forEach(GridDhtLocalPartition::clearAsync);

        doSleep(100);

        parts.get(parts.size() - 1).clearAsync();

        check(logLsnr, parts, parseParts);
    }

    /**
     * Test checks the presence of evicted partitions (MOVING state) in log without duplicate partitions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictPartByMovingState() throws Exception {
        backups = 1;

        IgniteEx node = startGrids(3);
        awaitPartitionMapExchange();

        stopGrid(2);

        awaitPartitionMapExchange();

        Map<Integer, Collection<Integer>> parseParts = new ConcurrentHashMap<>();

        LogListener logLsnr = logListener("clearing", parseParts, DEFAULT_CACHE_NAMES);
        testLog.registerListener(logLsnr);

        List<GridCacheAdapter<Object, Object>> internalCaches = of(DEFAULT_CACHE_NAMES)
            .map(node::cache)
            .map(GridCommonAbstractTest::internalCache0)
            .collect(toList());

        List<RebalanceFuture> rebFuts = internalCaches.stream()
            .map(internalCache -> (RebalanceFuture)internalCache.context().preloader().rebalanceFuture())
            .collect(toList());

        rebFuts.forEach(GridFutureAdapter::reset);

        List<GridDhtLocalPartition> parts = of(DEFAULT_CACHE_NAMES)
            .map(node::cache)
            .map(GridCommonAbstractTest::internalCache0)
            .flatMap(internalCache -> internalCache.context().topology().localPartitions().stream())
            .peek(p -> p.setState(MOVING))
            .collect(toList());

        parts.subList(0, parts.size() - 1).forEach(GridDhtLocalPartition::clearAsync);

        rebFuts.forEach(rebFut -> {
            GridTestUtils.setFieldValue(rebFut, "next", null);

            rebFut.onDone(Boolean.TRUE);
        });

        doSleep(100);
        rebFuts.forEach(GridFutureAdapter::reset);

        parts.get(parts.size() - 1).clearAsync();
        rebFuts.forEach(rebFut -> rebFut.onDone(Boolean.TRUE));

        check(logLsnr, parts, parseParts);
    }

    /**
     * Checking for logs without duplicate partitions.
     *
     * @param logLsnr Log listener.
     * @param parts Partitions.
     * @param parseParts Parsed partitions from the logs.
     */
    private void check(
        LogListener logLsnr,
        Collection<GridDhtLocalPartition> parts,
        Map<Integer, Collection<Integer>> parseParts
    ) {
        assertNotNull(logLsnr);
        assertNotNull(parts);
        assertNotNull(parseParts);

        assertTrue(logLsnr.check());

        Map<Integer, List<Integer>> partsByGrpId = parts.stream()
            .collect(groupingBy(p -> p.group().groupId(), mapping(GridDhtLocalPartition::id, toList())));

        partsByGrpId.forEach((grpId, partIds) -> {
            assertTrue(parseParts.containsKey(grpId));

            List<Integer> parsePartIds = new ArrayList<>(parseParts.get(grpId));

            Collections.sort(parsePartIds);
            Collections.sort(partIds);

            assertEqualsCollections(partIds, parsePartIds);
        });
    }

    /**
     * Creating a listener for logs with parsing of partitions.
     *
     * @param reason Reason to eviction.
     * @param evictParts To collect parsed partitions.
     * @param cacheNames Cache names.
     * @return Log Listener.
     */
    private LogListener logListener(
        String reason,
        Map<Integer, Collection<Integer>> evictParts,
        String... cacheNames
    ) {
        assertNotNull(reason);
        assertNotNull(evictParts);
        assertNotNull(cacheNames);

        List<String> cacheInfos = of(cacheNames)
            .map(cacheName -> "grpId=" + CU.cacheId(cacheName) + ", grpName=" + cacheName)
            .collect(toList());

        Pattern extractParts = Pattern.compile(reason + "=\\[([0-9\\-,]*)]");
        Pattern extractGrpId = Pattern.compile("grpId=([0-9]*)");

        LogListener.Builder builder = LogListener.matches(logStr -> {
            String msgPrefix = "Partitions have been scheduled for eviction:";
            if (!logStr.contains(msgPrefix))
                return false;

            of(logStr.replace(msgPrefix, "").split("], \\[")).forEach(s -> {

                Matcher grpIdMatcher = extractGrpId.matcher(s);
                Matcher partsMatcher = extractParts.matcher(s);

                //find and parsing grpId and partitions
                if (grpIdMatcher.find() && partsMatcher.find()) {
                    evictParts.computeIfAbsent(parseInt(grpIdMatcher.group(1)), i -> new ConcurrentLinkedQueue<>())
                        .addAll(parseContentCompactStr(partsMatcher.group(1)));
                }
            });

            return cacheInfos.stream().allMatch(logStr::contains);
        });

        return builder.build();
    }

    /**
     * Parse contents of compact string after a {@link S#compact(Collection)}.
     *
     * @param str Compact string content.
     * @return Parsed numbers.
     */
    private Collection<Integer> parseContentCompactStr(String str) {
        assertNotNull(str);

        return of(str.split(","))
            .map(String::trim)
            .flatMap(s -> {
                if (s.contains("-")) {
                    String[] range = s.split("-");

                    return IntStream.rangeClosed(parseInt(range[0]), parseInt(range[1])).boxed();
                }
                else
                    return of(parseInt(s));
            }).collect(toList());
    }
}
