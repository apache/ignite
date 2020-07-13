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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Organization;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Person;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_SIZES;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakCacheDataTree;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakSqlIndex;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.organizationEntity;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.personEntity;

/**
 * Class for testing function of checking size of index and cache in
 * {@link CacheSubcommands#VALIDATE_INDEXES}.
 */
public class GridCommandHandlerIndexingCheckSizeTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Entry count for entity. */
    private static final int ENTRY_CNT = 100;

    /** Non persistent data region name. */
    private static final String NON_PERSIST_REGION = "non-persist";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        createAndFillCache(client, CACHE_NAME, GROUP_NAME, null, queryEntities(), ENTRY_CNT);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setDataRegionConfigurations(
            new DataRegionConfiguration().setName(NON_PERSIST_REGION)
        );

        return cfg;
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when cache is broken.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenCache() {
        validateCheckSizesAfterBreakCacheDataTree(crd, CACHE_NAME, ENTRY_CNT);
    }

    /**
     * Test checks that cache size and index validation error will not be
     * displayed if cache is broken, because argument
     * {@link ValidateIndexesCommandArg#CHECK_SIZES} not used.
     */
    @Test
    public void testNoCheckCacheSizeWhenBrokenCache() {
        String cacheName = CACHE_NAME;

        breakCacheDataTree(log, crd.cachex(cacheName), 1, null);

        checkNoCheckSizeInCaseBrokenData(cacheName);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when index is broken.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenIdx() throws Exception {
        validateCheckSizesAfterBreakSqlIndex(crd, CACHE_NAME, ENTRY_CNT);
    }

    /**
     * Test checks that cache size and index validation error will not be
     * displayed if index is broken, because argument
     * {@link ValidateIndexesCommandArg#CHECK_SIZES} not used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoCheckCacheSizeWhenBrokenIdx() throws Exception {
        String cacheName = CACHE_NAME;

        breakSqlIndex(crd.cachex(cacheName), 1, null);

        checkNoCheckSizeInCaseBrokenData(cacheName);
    }

    /**
     * Test that checks that there will be no errors when executing command
     * "validate_indexes" with/without "--check-sizes" on the cache without
     * {@link QueryEntity}.
     */
    @Test
    public void testNoErrorOnCacheWithoutQueryEntity() {
        String cacheName = DEFAULT_CACHE_NAME;

        createAndFillCache(crd, cacheName, GROUP_NAME, null, emptyMap(), 0);

        try (IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(cacheName)) {
            for (int i = 0; i < ENTRY_CNT; i++)
                streamer.addData(i, new Person(i, "p_" + i));

            streamer.flush();
        }

        execVIWithNoErrCheck(cacheName, false);
        execVIWithNoErrCheck(cacheName, true);
    }

    /**
     * Test that checks that there will be no errors when executing command
     * "validate_indexes" with/without "--check-sizes" on the empty cache
     * with {@link QueryEntity}.
     */
    @Test
    public void testNoErrorOnEmptyCacheWithQueryEntity() {
        String cacheName = DEFAULT_CACHE_NAME;

        createAndFillCache(crd, cacheName, GROUP_NAME, null, queryEntities(), 0);

        execVIWithNoErrCheck(cacheName, false);
        execVIWithNoErrCheck(cacheName, true);
    }

    /**
     * Test checks that there will be no errors if there are entries without
     * {@link QueryEntity} in cache.
     */
    @Test
    public void testNoErrorOnCacheWithEntryWithoutQueryEntity() {
        String cacheName = CACHE_NAME;

        int cacheSize = crd.cachex(cacheName).size();

        try (IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(cacheName)) {
            for (int i = cacheSize; i < cacheSize + ENTRY_CNT; i++)
                streamer.addData(i, i);

            streamer.flush();
        }

        execVIWithNoErrCheck(cacheName, false);
        execVIWithNoErrCheck(cacheName, true);
    }

    /**
     * Test checks that there will be no errors if there are entries without
     * {@link QueryEntity} in cache, and also if there are null values.
     */
    @Test
    public void testNoErrorOnCacheWithEntryWithoutQueryEntityAndWithNullValues() {
        String cacheName = CACHE_NAME;

        int cacheSize = crd.cachex(cacheName).size();

        try (IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(cacheName)) {
            int i = cacheSize;

            for (; i < cacheSize + (ENTRY_CNT - 10); i++)
                streamer.addData(i, i);

            for (; i < cacheSize + ENTRY_CNT; i++)
                streamer.addData(i, null);

            streamer.flush();
        }

        execVIWithNoErrCheck(cacheName, false);
        execVIWithNoErrCheck(cacheName, true);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when cache is broken. In {@link #NON_PERSIST_REGION} region.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenCacheInNonPersistRegion() {
        IgniteEx node = crd;
        String cacheName = CACHE_NAME + "_new";

        createAndFillCache(node, cacheName, GROUP_NAME + "_new", NON_PERSIST_REGION, queryEntities(), ENTRY_CNT);

        validateCheckSizesAfterBreakCacheDataTree(node, cacheName, ENTRY_CNT);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when index is broken. In {@link #NON_PERSIST_REGION} region.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenIdxInNonPersistRegion() throws Exception {
        IgniteEx node = crd;
        String cacheName = CACHE_NAME + "_new";

        createAndFillCache(node, cacheName, GROUP_NAME + "_new", NON_PERSIST_REGION, queryEntities(), ENTRY_CNT);

        validateCheckSizesAfterBreakSqlIndex(node, cacheName, ENTRY_CNT);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when cache is broken. In case of dynamic add a column and index.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenCacheWithDynamicAddColumnAndIndex() {
        IgniteEx node = crd;
        String cacheName = CACHE_NAME;

        int addCnt = ENTRY_CNT;
        addColumnAndIdx(node, cacheName, addCnt);

        validateCheckSizesAfterBreakCacheDataTree(node, cacheName, ENTRY_CNT + addCnt);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when index is broken. In case of dynamic add a column and index.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenIdxWithDynamicAddColumnAndIndex() throws Exception {
        IgniteEx node = crd;
        String cacheName = CACHE_NAME;

        int addCnt = ENTRY_CNT;
        addColumnAndIdx(node, cacheName, addCnt);

        validateCheckSizesAfterBreakSqlIndex(node, cacheName, ENTRY_CNT + addCnt);
    }

    /**
     * Test that checks that there will be no errors when executing command
     * "validate_indexes" with/without "--check-sizes" on cache with
     * {@link QueryEntity}.
     */
    @Test
    public void testNoErrorOnCacheWithQueryEntity() {
        String cacheName = CACHE_NAME;

        execVIWithNoErrCheck(cacheName, false);
        execVIWithNoErrCheck(cacheName, true);
    }

    /**
     * Adding the "address" column and index for {@link Person} and
     * {@link Organization}, with new entries added for each of them.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param addCnt How many entries add to table.
     * */
    private void addColumnAndIdx(IgniteEx node, String cacheName, int addCnt) {
        IgniteCache<Object, Object> cache = node.cache(cacheName);

        cache.query(new SqlFieldsQuery("alter table Person add column orgAddr varchar")).getAll();
        cache.query(new SqlFieldsQuery("alter table Organization add column addr varchar")).getAll();

        cache.query(new SqlFieldsQuery("create index p_o_addr on Person (orgAddr)")).getAll();
        cache.query(new SqlFieldsQuery("create index o_addr on Organization (addr)")).getAll();

        int key = node.cachex(cacheName).size();

        try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(cacheName)) {
            ThreadLocalRandom rand = ThreadLocalRandom.current();

            for (int i = 0; i < addCnt; i++) {
                streamer.addData(
                    key++,
                    new Person(rand.nextInt(), valueOf(rand.nextLong())).orgAddr(valueOf(rand.nextLong()))
                );

                streamer.addData(
                    key++,
                    new Organization(rand.nextInt(), valueOf(rand.nextLong())).addr(valueOf(rand.nextLong()))
                );
            }

            streamer.flush();
        }
    }

    /**
     * Validation of cache and index sizes after breaking sql index.
     *
     * @param node      Node.
     * @param cacheName Cache name.
     * @param entryCnt  Entry count for table.
     */
    private void validateCheckSizesAfterBreakSqlIndex(IgniteEx node, String cacheName, int entryCnt) throws Exception {
        Map<String, AtomicInteger> rmvIdxByTbl = new HashMap<>();

        breakSqlIndex(node.cachex(cacheName), 0, row -> {
            rmvIdxByTbl.computeIfAbsent(tableName(cacheName, row), s -> new AtomicInteger()).incrementAndGet();
            return true;
        });

        assertEquals(rmvIdxByTbl.size(), queryEntities().size());
        validateCheckSizes(node, cacheName, rmvIdxByTbl, ai -> entryCnt, ai -> entryCnt - ai.get());
    }

    /**
     * Validation of cache and index sizes after breaking CacheDataTree.
     *
     * @param node      Node.
     * @param cacheName Cache name.
     * @param entryCnt  Entry count for table.
     */
    private void validateCheckSizesAfterBreakCacheDataTree(IgniteEx node, String cacheName, int entryCnt) {
        requireNonNull(cacheName);
        requireNonNull(node);

        Map<String, AtomicInteger> rmvEntryByTbl = new HashMap<>();

        breakCacheDataTree(log, node.cachex(cacheName), 1, (i, entry) -> {
            rmvEntryByTbl.computeIfAbsent(tableName(cacheName, entry), s -> new AtomicInteger()).incrementAndGet();
            return true;
        });

        assertEquals(rmvEntryByTbl.size(), queryEntities().size());
        validateCheckSizes(node, cacheName, rmvEntryByTbl, ai -> entryCnt - ai.get(), ai -> entryCnt);
    }

    /**
     * Creating {@link QueryEntity}'s with filling functions.
     *
     * @return {@link QueryEntity}'s with filling functions.
     */
    private Map<QueryEntity, Function<Random, Object>> queryEntities() {
        Map<QueryEntity, Function<Random, Object>> qryEntities = new HashMap<>();

        qryEntities.put(personEntity(), rand -> new Person(rand.nextInt(), valueOf(rand.nextLong())));
        qryEntities.put(organizationEntity(), rand -> new Organization(rand.nextInt(), valueOf(rand.nextLong())));

        return qryEntities;
    }

    /**
     * Executing "validate_indexes" command with verify that there are
     * no errors in result.
     *
     * @param cacheName Cache name.
     * @param checkSizes Add argument "--check-sizes".
     */
    private void execVIWithNoErrCheck(String cacheName, boolean checkSizes) {
        List<String> cmdWithArgs = new ArrayList<>(asList(CACHE.text(), VALIDATE_INDEXES.text(), cacheName));

        if (checkSizes)
            cmdWithArgs.add(CHECK_SIZES.argName());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(cmdWithArgs));

        String out = testOut.toString();
        assertNotContains(log, out, "issues found (listed above)");
        assertNotContains(log, out, "Size check");
    }

    /**
     * Check that if data is broken and option
     * {@link ValidateIndexesCommandArg#CHECK_SIZES} is enabled, size check
     * will not take place.
     *
     * @param cacheName Cache size.
     */
    private void checkNoCheckSizeInCaseBrokenData(String cacheName) {
        injectTestSystemOut();

        assertEquals(
            EXIT_CODE_OK,
            execute(CACHE.text(), VALIDATE_INDEXES.text(), cacheName)
        );

        String out = testOut.toString();
        assertContains(log, out, "issues found (listed above)");
        assertNotContains(log, out, "Size check");
    }

    /**
     * Checking whether cache and index size check is correct.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param rmvByTbl Number of deleted items per table.
     * @param cacheSizeExp Function for getting expected cache size.
     * @param idxSizeExp Function for getting expected index size.
     */
    private void validateCheckSizes(
        IgniteEx node,
        String cacheName,
        Map<String, AtomicInteger> rmvByTbl,
        Function<AtomicInteger, Integer> cacheSizeExp,
        Function<AtomicInteger, Integer> idxSizeExp
    ) {
        requireNonNull(node);
        requireNonNull(cacheName);
        requireNonNull(rmvByTbl);
        requireNonNull(cacheSizeExp);
        requireNonNull(idxSizeExp);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(CACHE.text(), VALIDATE_INDEXES.text(), cacheName, CHECK_SIZES.argName()));

        String out = testOut.toString();
        assertContains(log, out, "issues found (listed above)");
        assertContains(log, out, "Size check");

        Map<String, ValidateIndexesCheckSizeResult> valIdxCheckSizeResults =
            ((VisorValidateIndexesTaskResult)lastOperationResult).results().get(node.localNode().id())
                .checkSizeResult();

        assertEquals(rmvByTbl.size(), valIdxCheckSizeResults.size());

        for (Map.Entry<String, AtomicInteger> rmvByTblEntry : rmvByTbl.entrySet()) {
            ValidateIndexesCheckSizeResult checkSizeRes = valIdxCheckSizeResults.entrySet().stream()
                .filter(e -> e.getKey().contains(rmvByTblEntry.getKey()))
                .map(Map.Entry::getValue)
                .findAny()
                .orElse(null);

            assertNotNull(checkSizeRes);
            assertEquals((int)cacheSizeExp.apply(rmvByTblEntry.getValue()), checkSizeRes.cacheSize());

            Collection<ValidateIndexesCheckSizeIssue> issues = checkSizeRes.issues();
            assertFalse(issues.isEmpty());

            issues.forEach(issue -> {
                assertEquals((int)idxSizeExp.apply(rmvByTblEntry.getValue()), issue.indexSize());

                Throwable err = issue.error();
                assertNotNull(err);
                assertEquals("Cache and index size not same.", err.getMessage());
            });
        }
    }

    /**
     * Get table name for cache row.
     *
     * @param cacheName  Cache name.
     * @param cacheDataRow Cache row.
     */
    private String tableName(String cacheName, CacheDataRow cacheDataRow) {
        requireNonNull(cacheName);
        requireNonNull(cacheDataRow);

        try {
            return crd.context().query().typeByValue(
                cacheName,
                crd.cachex(cacheName).context().cacheObjectContext(),
                cacheDataRow.key(),
                cacheDataRow.value(),
                false
            ).tableName();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Get table name for cache entry.
     *
     * @param cacheName  Cache name.
     * @param cacheEntry Cache entry.
     */
    private <K, V> String tableName(String cacheName, Cache.Entry<K, V> cacheEntry) {
        requireNonNull(cacheName);
        requireNonNull(cacheEntry);

        try {
            return crd.context().query().typeByValue(
                cacheName,
                crd.cachex(cacheName).context().cacheObjectContext(),
                null,
                ((CacheObject)cacheEntry.getValue()),
                false
            ).tableName();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
