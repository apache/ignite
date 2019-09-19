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

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
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
     * Tests that validation doesn't fail if nothing is broken.
     */
    @Test
    public void testValidateIndexesNoErrors() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

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
        breakCacheDataTree(crd, CACHE_NAME, 1);

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
     * Tests that missing rows in H2 indexes are detected.
     */
    @Test
    public void testBrokenSqlIndexShouldFailValidation() throws Exception {
        breakSqlIndex(crd, CACHE_NAME);

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

        breakCacheDataTree(crd, CACHE_NAME, 1);

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
     * Removes some entries from a partition skipping index update. This effectively breaks the index.
     */
    private void breakCacheDataTree(Ignite ig, String cacheName, int partId) {
        IgniteEx ig0 = (IgniteEx)ig;
        int cacheId = CU.cacheId(cacheName);

        ScanQuery scanQry = new ScanQuery(partId);

        GridCacheContext<Object, Object> ctx = ig0.context().cache().context().cacheContext(cacheId);

        // Get current update counter
        String grpName = ig0.context().cache().context().cacheContext(cacheId).config().getGroupName();
        int cacheGrpId = grpName == null ? cacheName.hashCode() : grpName.hashCode();

        GridDhtLocalPartition locPart = ctx.dht().topology().localPartition(partId);
        IgniteCacheOffheapManager.CacheDataStore dataStore = ig0.context().cache().context().cache().cacheGroup(cacheGrpId).offheap().dataStore(locPart);

        Iterator<Cache.Entry> it = ig.cache(cacheName).withKeepBinary().query(scanQry).iterator();

        for (int i = 0; i < 5_000; i++) {
            if (it.hasNext()) {
                Cache.Entry entry = it.next();

                if (i % 5 == 0) {
                    // Do update
                    GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();

                    db.checkpointReadLock();

                    try {
                        IgniteCacheOffheapManager.CacheDataStore innerStore = U.field(dataStore, "delegate");

                        // IgniteCacheOffheapManagerImpl.CacheDataRowStore
                        Object rowStore = U.field(innerStore, "rowStore");

                        // IgniteCacheOffheapManagerImpl.CacheDataTree
                        Object dataTree = U.field(innerStore, "dataTree");

                        CacheDataRow oldRow = U.invoke(
                            dataTree.getClass(),
                            dataTree,
                            "remove",
                            new SearchRow(cacheId, ctx.toCacheKeyObject(entry.getKey())));

                        if (oldRow != null)
                            U.invoke(rowStore.getClass(), rowStore, "removeRow", oldRow.link(), IoStatisticsHolderNoOp.INSTANCE);
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Failed to remove key skipping indexes: " + entry);

                        e.printStackTrace();
                    }
                    finally {
                        db.checkpointReadUnlock();
                    }
                }
            }
            else {
                log.info("Early exit for index corruption, keys processed: " + i);

                break;
            }
        }
    }

    /**
     * Removes some entries from H2 trees skipping partition updates. This effectively breaks the index.
     */
    private void breakSqlIndex(Ignite ig, String cacheName) throws Exception {
        GridQueryProcessor qry = ((IgniteEx)ig).context().query();

        GridCacheContext<Object, Object> ctx = ((IgniteEx)ig).cachex(cacheName).context();

        GridDhtLocalPartition locPart = ctx.topology().localPartitions().get(0);

        GridIterator<CacheDataRow> it = ctx.group().offheap().partitionIterator(locPart.id());

        for (int i = 0; i < 500; i++) {
            if (!it.hasNextX()) {
                log.info("Early exit for index corruption, keys processed: " + i);

                break;
            }

            CacheDataRow row = it.nextX();

            ctx.shared().database().checkpointReadLock();

            try {
                qry.remove(ctx, row);
            }
            finally {
                ctx.shared().database().checkpointReadUnlock();
            }
        }
    }
}
