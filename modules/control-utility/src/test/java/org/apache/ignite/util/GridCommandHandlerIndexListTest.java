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

import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.visor.cache.index.IndexListInfoContainer;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME_SECOND;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME_SECOND;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.THREE_ENTRIES_CACHE_NAME_COMMON_PART;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillSeveralCaches;

/**
 * Test for --cache index_list command. Uses single cluster per suite.
 */
public class GridCommandHandlerIndexListTest extends GridCommandHandlerAbstractTest {
    /** Grids number. */
    public static final int GRIDS_NUM = 2;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = startGrids(GRIDS_NUM);

        ignite.cluster().active(true);

        createAndFillSeveralCaches(ignite);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * Tests --index name option and output correctness.
     */
    @Test
    public void testCacheIndexList() {
        final String idxName = "PERSON_ORGID_ASC_IDX";
        final int expectedLinesNum = 15;
        final int expectedIndexDescrLinesNum = 2;

        injectTestSystemOut();

        final CommandHandler handler = new CommandHandler(createTestLogger());

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_list", "--index-name", idxName));

        String outStr = testOut.toString();

        assertTrue(outStr.contains("grpName=" + GROUP_NAME + ", cacheName=" + CACHE_NAME + ", idxName=PERSON_ORGID_ASC_IDX, " +
            "colsNames=ArrayList [ORGID, _KEY], tblName=PERSON"));
        assertTrue(outStr.contains("grpName=" + GROUP_NAME_SECOND + ", cacheName=" + CACHE_NAME_SECOND +
            ", idxName=PERSON_ORGID_ASC_IDX, colsNames=ArrayList [ORGID, _KEY], tblName=PERSON"));

        final String[] outputLines = outStr.split("\n");

        int outputLinesNum = outputLines.length;

        assertEquals("Unexpected number of lines: " + outputLinesNum, outputLinesNum, expectedLinesNum);

        long indexDescrLinesNum =
            Arrays.stream(outputLines)
                .filter(s -> s.contains("grpName="))
                .count();

        assertEquals("Unexpected number of index description lines: " + indexDescrLinesNum,
            indexDescrLinesNum, expectedIndexDescrLinesNum);

        Set<IndexListInfoContainer> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);

        final int resSetSize = cmdResult.size();
        assertEquals("Unexpected result set size: " + resSetSize, resSetSize, 2);

        boolean isResSetCorrect =
            cmdResult.stream()
                .map(IndexListInfoContainer::indexName)
                .allMatch((name) -> name.equals(idxName));

        assertTrue("Unexpected result set", isResSetCorrect);
    }

    /**
     * Checks that command with all arguments specified works.
     */
    @Test
    public void tesAllArgs() {
        final String idxName = "PERSON_ORGID_ASC_IDX";

        injectTestSystemOut();

        final CommandHandler handler = new CommandHandler(createTestLogger());

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_list",
            "--node-id", grid(0).localNode().id().toString(),
            "--group-name", "^" + GROUP_NAME + "$",
            "--cache-name", CACHE_NAME,
            "--index-name", idxName));

        assertTrue(testOut.toString().contains("grpName=" + GROUP_NAME + ", cacheName=" + CACHE_NAME +
            ", idxName=PERSON_ORGID_ASC_IDX, colsNames=ArrayList [ORGID, _KEY], tblName=PERSON"));
    }

    /**
     * Checks that command works for cache without indexes.
     */
    @Test
    public void testListCacheWithoutIndexes() {
        IgniteEx ignite = grid(0);

        final String tmpCacheName = "tmpCache";

        injectTestSystemOut();

        final CommandHandler handler = new CommandHandler(createTestLogger());

        try {
            ignite.createCache(tmpCacheName);

            try (IgniteDataStreamer<Long, Long> streamer = ignite.dataStreamer(tmpCacheName)) {
                for (long i = 0; i < 1000; i++)
                    streamer.addData(i * 13, i * 17);
            }

            assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_list", "--cache-name", tmpCacheName));

            String str = testOut.toString();

            String[] lines = str.split("\n");

            assertEquals("Unexpected output size", 11, lines.length);
        }
        finally {
            ignite.destroyCache(tmpCacheName);
        }
    }

    /**
     * Checks --group-name filter.
     * Expect to see all indexes for caches in groups "group1".
     * 7 indexes in total.
     */
    @Test
    public void testGroupNameFilter1() {
        checkGroup("^" + GROUP_NAME + "$", GROUP_NAME, 7);
    }

    /**
     * Checks --group-name filter.
     * Expect to see all indexes for caches in groups "group1" and "group1_second".
     * 10 indexes in total.
     */
    @Test
    public void testGroupNameFilter2() {
        checkGroup("^" + GROUP_NAME, (name) -> name.equals(GROUP_NAME) || name.equals(GROUP_NAME_SECOND), 10);
    }

    /**
     * Checks --group-name filter for caches without explicitly specified group.
     * Expect to see 4(3 custom and 1 PK) indexes of three_entries_simple_indexes cache.
     */
    @Test
    public void testEmptyGroupFilter() {
        checkGroup(CacheCommands.EMPTY_GROUP_NAME, CacheCommands.EMPTY_GROUP_NAME, 4);
    }

    /** */
    private void checkGroup(String grpRegEx, String grpName, int expectedResNum) {
        checkGroup(grpRegEx, (name) -> name.equals(grpName), expectedResNum);
    }

    /** */
    private void checkGroup(String grpRegEx, Predicate<String> predicate, int expectedResNum) {
        final CommandHandler handler = new CommandHandler(createTestLogger());

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_list", "--group-name", grpRegEx));

        Set<IndexListInfoContainer> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);

        boolean isResCorrect =
            cmdResult.stream()
                .map(IndexListInfoContainer::groupName)
                .allMatch(predicate);

        assertTrue("Unexpected command result", isResCorrect);

        int indexesNum = cmdResult.size();
        assertEquals("Unexpected number of indexes: " + indexesNum, indexesNum, expectedResNum);
    }

    /**
     * Checks --cache-name filter.
     * Expect to see 4(3 custom and 1 PK) indexes of three_entries_simple_indexes cache and
     * 4(3 custom and 1 PK) indexes of test_three_entries_complex_indexes cache
     * */
    @Test
    public void testCacheNameFilter1() {
        checkCacheNameFilter(
            THREE_ENTRIES_CACHE_NAME_COMMON_PART,
            cacheName -> cacheName.contains(THREE_ENTRIES_CACHE_NAME_COMMON_PART),
            8
        );
    }

    /**
     * Checks --cache-name filter.
     * Expect to see 4(3 custom and 1 PK) indexes of three_entries_simple_indexes cache.
     * */
    @Test
    public void testCacheNameFilter2() {
        checkCacheNameFilter(
            "^" + THREE_ENTRIES_CACHE_NAME_COMMON_PART,
            cacheName -> cacheName.startsWith(THREE_ENTRIES_CACHE_NAME_COMMON_PART),
            4
        );
    }

    /** */
    private void checkCacheNameFilter(String cacheRegEx, Predicate<String> predicate, int expectedResNum) {
        final CommandHandler handler = new CommandHandler(createTestLogger());

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_list", "--cache-name", cacheRegEx));

        Set<IndexListInfoContainer> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);

        boolean isResCorrect =
            cmdResult.stream()
                .map(IndexListInfoContainer::cacheName)
                .allMatch(predicate);

        assertTrue("Unexpected command result", isResCorrect);

        int indexesNum = cmdResult.size();
        assertEquals("Unexpected number of indexes: " + indexesNum, indexesNum, expectedResNum);
    }
}
