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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class PageMemoryLazyAllocationTest extends GridCommonAbstractTest {
    /** */
    public static final String LAZY_REGION = "lazyRegion";

    /** */
    public static final String EAGER_REGION = "eagerRegion";

    /** */
    protected boolean lazyAllocation = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(LAZY_REGION)
                    .setLazyMemoryAllocation(lazyAllocation)
                    .setPersistenceEnabled(persistenceEnabled()),
                new DataRegionConfiguration()
                    .setName(EAGER_REGION)
                    .setLazyMemoryAllocation(lazyAllocation)
                    .setPersistenceEnabled(persistenceEnabled())));

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>("my-cache")
            .setDataRegionName(EAGER_REGION);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testLazyMemoryAllocationOnServer() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startSrv()[0];

        IgniteCacheDatabaseSharedManager db = srv.context().cache().context().database();

        checkMemoryAllocated(db.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(db.dataRegion(LAZY_REGION).pageMemory());

        createCacheAndPut(srv);

        checkMemoryAllocated(db.dataRegion(LAZY_REGION).pageMemory());
    }

    /** @throws Exception If failed. */
    @Test
    public void testLazyMemoryAllocationOnClient() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startSrv()[0];

        IgniteCacheDatabaseSharedManager srvDb = srv.context().cache().context().database();

        checkMemoryAllocated(srvDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(srvDb.dataRegion(LAZY_REGION).pageMemory());

        IgniteEx clnt = startClientGrid(2);

        IgniteCacheDatabaseSharedManager clntDb = clnt.context().cache().context().database();

        checkMemoryNotAllocated(clntDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(clntDb.dataRegion(LAZY_REGION).pageMemory());

        createCacheAndPut(clnt);

        checkMemoryAllocated(srvDb.dataRegion(LAZY_REGION).pageMemory());

        checkMemoryNotAllocated(clntDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(clntDb.dataRegion(LAZY_REGION).pageMemory());
    }

    /** @throws Exception If failed. */
    @Test
    public void testEagerMemoryAllocationOnServer() throws Exception {
        lazyAllocation = false;

        IgniteEx g = startSrv()[0];

        IgniteCacheDatabaseSharedManager db = g.context().cache().context().database();

        checkMemoryAllocated(db.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryAllocated(db.dataRegion(LAZY_REGION).pageMemory());

        createCacheAndPut(g);
    }

    /** @throws Exception If failed. */
    @Test
    public void testEagerMemoryAllocationOnClient() throws Exception {
        lazyAllocation = false;

        IgniteEx srv = startSrv()[0];

        IgniteCacheDatabaseSharedManager srvDb = srv.context().cache().context().database();

        checkMemoryAllocated(srvDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryAllocated(srvDb.dataRegion(LAZY_REGION).pageMemory());

        IgniteEx clnt = startClientGrid(2);

        IgniteCacheDatabaseSharedManager clntDb = clnt.context().cache().context().database();

        checkMemoryNotAllocated(clntDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(clntDb.dataRegion(LAZY_REGION).pageMemory());

        createCacheAndPut(clnt);

        checkMemoryNotAllocated(clntDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(clntDb.dataRegion(LAZY_REGION).pageMemory());
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocalCacheOnClientNodeWithLazyAllocation() throws Exception {
        lazyAllocation = true;

        IgniteEx srv = startSrv()[0];

        IgniteCacheDatabaseSharedManager srvDb = srv.context().cache().context().database();

        checkMemoryAllocated(srvDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(srvDb.dataRegion(LAZY_REGION).pageMemory());

        IgniteEx clnt = startClientGrid(2);

        IgniteCacheDatabaseSharedManager clntDb = clnt.context().cache().context().database();

        checkMemoryNotAllocated(clntDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(clntDb.dataRegion(LAZY_REGION).pageMemory());

        createCacheAndPut(clnt, CacheMode.LOCAL);

        checkMemoryNotAllocated(clntDb.dataRegion(EAGER_REGION).pageMemory());
        //LOCAL Cache was created in LAZY_REGION so it has to be allocated on client node.
        checkMemoryAllocated(clntDb.dataRegion(LAZY_REGION).pageMemory());
    }

    /** @throws Exception If failed. */
    @Test
    public void testStopNotAllocatedRegions() throws Exception {
        IgniteEx srv = startSrv()[0];

        IgniteCacheDatabaseSharedManager srvDb = srv.context().cache().context().database();

        checkMemoryAllocated(srvDb.dataRegion(EAGER_REGION).pageMemory());
        checkMemoryNotAllocated(srvDb.dataRegion(LAZY_REGION).pageMemory());

        stopGrid(0);
    }

    @After
    public void after() {
        stopAllGrids();
    }

    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    protected void createCacheAndPut(IgniteEx g) {
        createCacheAndPut(g, CacheConfiguration.DFLT_CACHE_MODE);
    }

    /** */
    private void createCacheAndPut(IgniteEx g, CacheMode cacheMode) {
        createCacheAndPut(g, cacheMode, null);
    }

    /** */
    private void createCacheAndPut(IgniteEx g, CacheMode cacheMode, IgnitePredicate<ClusterNode> fltr) {
        IgniteCache<Integer, String> cache =
            g.createCache(new CacheConfiguration<Integer, String>("my-cache-2")
                .setCacheMode(cacheMode)
                .setDataRegionName(LAZY_REGION)
                .setNodeFilter(fltr));

        cache.put(1, "test");

        assertEquals(cache.get(1), "test");
    }

    /** */
    protected void checkMemoryAllocated(PageMemory pageMem) {
        Object[] segments = GridTestUtils.getFieldValue(pageMem, "segments");

        assertNotNull(segments);
        assertTrue(segments.length > 0);
        assertNotNull(segments[0]);
    }

    /** */
    protected void checkMemoryNotAllocated(PageMemory pageMem) {
        Object[] segments = GridTestUtils.getFieldValue(pageMem, "segments");

        assertNull(segments);
    }

    /** */
    protected IgniteEx[] startSrv() throws Exception {
        IgniteEx srv0 = startGrid(0);
        IgniteEx srv1 = startGrid(1);

        srv0.cluster().active(true);

        awaitPartitionMapExchange();

        return new IgniteEx[] {srv0, srv1};
    }

    /** */
    protected boolean persistenceEnabled() {
        return false;
    }
}
