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

import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.cache.CheckIndexInlineSizes.INDEXES_INLINE_SIZE_ARE_THE_SAME;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakCacheDataTree;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakSqlIndex;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;

/**
 * You can use this class if you don't need create nodes for each test because
 * here create {@link #SERVER_NODE_CNT} server and 1 client nodes at before all
 * tests. If you need create nodes for each test you can use
 * {@link GridCommandHandlerIndexingTest}.
 */
public class GridCommandHandlerIndexingClusterByClassTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        createAndFillCache(client, CACHE_NAME, GROUP_NAME);
    }

    /**
     * Tests --cache check_index_inline_sizes works in case of all indexes have the same inline size.
     */
    @Test
    public void testCheckIndexInlineSizesNoError() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "check_index_inline_sizes"));

        String output = testOut.toString();

        assertContains(log, output, "Found 2 secondary indexes.");
        assertContains(log, output, INDEXES_INLINE_SIZE_ARE_THE_SAME);
    }

    /**
     * Tests that validation doesn't fail if nothing is broken.
     */
    @Test
    public void testValidateIndexesNoErrors() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertContains(log, testOut.toString(), "no issues found");
    }

    /**
     * Tests that validation with CRC checking doesn't fail if nothing is broken.
     */
    @Test
    public void testValidateIndexesWithCrcNoErrors() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", CACHE_NAME));

        assertContains(log, testOut.toString(), "no issues found");
    }

    /**
     * Test verifies that validate_indexes command finishes successfully when no cache names are specified.
     */
    @Test
    public void testValidateIndexesNoErrorEmptyCacheNameArg() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes"));

        assertContains(log, testOut.toString(), "no issues found");
    }

    /**
     * Tests that missing rows in CacheDataTree are detected.
     */
    @Test
    public void testBrokenCacheDataTreeShouldFailValidation() {
        breakCacheDataTreeOnCrd();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK,
            execute(
                "--cache",
                "validate_indexes",
                CACHE_NAME,
                "--check-first", "10000",
                "--check-through", "10"));

        String out = testOut.toString();

        assertContains(log, out, "issues found (listed above)");

        assertContains(log, out, "Key is present in SQL index, but is missing in corresponding data page.");
    }

    /**
     * Checks that missing lines were detected in CacheDataTree with the output
     * of cache group name and id.
     */
    @Test
    public void testBrokenCacheDataTreeShouldFailValidationWithCacheGroupInfo() {
        breakCacheDataTreeOnCrd();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertContains(
            log,
            testOut.toString(),
            "[cacheGroup=group1, cacheGroupId=-1237460590, cache=persons-cache-vi, cacheId=-528791027, idx=_key_PK]"
        );
    }

    /**
     * Tests that missing rows in H2 indexes are detected.
     */
    @Test
    public void testBrokenSqlIndexShouldFailValidation() throws Exception {
        breakSqlIndexOnCrd();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertContains(log, testOut.toString(), "issues found (listed above)");
    }

    /**
     * Test to validate only specified cache, not all cache group.
     */
    @Test
    public void testValidateSingleCacheShouldNotTriggerCacheGroupValidation() throws Exception {
        createAndFillCache(crd, DEFAULT_CACHE_NAME, GROUP_NAME);

        forceCheckpoint();

        breakCacheDataTreeOnCrd();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", DEFAULT_CACHE_NAME, "--check-through", "10"));
        assertContains(log, testOut.toString(), "no issues found");
    }

    /**
     * Test validate_indexes with empty cache list.
     */
    @Test
    public void testCacheValidateIndexesPassEmptyCacheList() throws Exception {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes"));
        assertContains(log, testOut.toString(), "no issues found");
    }

    /**
     * Removes some entries from a partition skipping index update.
     */
    private void breakCacheDataTreeOnCrd() {
        breakCacheDataTree(log, crd.cachex(CACHE_NAME), 1, (i, entry) -> i % 5 == 0);
    }

    /**
     * Removes some entries from H2 trees skipping partition updates.
     * This effectively breaks the index.
     *
     * @throws Exception If failed.
     */
    private void breakSqlIndexOnCrd() throws Exception {
        breakSqlIndex(crd.cachex(CACHE_NAME), 0, null);
    }
}
