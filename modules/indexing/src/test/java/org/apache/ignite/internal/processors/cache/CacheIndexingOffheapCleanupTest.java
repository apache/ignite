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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;

/**
 *
 */
public class CacheIndexingOffheapCleanupTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheDestroy() throws Exception {
        Ignite ignite = ignite(0);

        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            IgniteCache cache = ignite.createCache(cacheConfiguration());

            for (int k = 0; k < 100; k++)
                cache.put(k, new TestType());

            GridUnsafeMemory mem = schemaMemory(ignite, cache.getName());

            assertTrue(mem.allocatedSize() > 0);

            ignite.destroyCache(cache.getName());

            assertEquals(0, mem.allocatedSize());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNode() throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache cache = ignite.createCache(cacheConfiguration());

        for (int k = 0; k < 100; k++)
            cache.put(k, new TestType());

        GridUnsafeMemory mem = schemaMemory(ignite, cache.getName());

        assertTrue(mem.allocatedSize() > 0);

        stopGrid(0);

        assertEquals(0, mem.allocatedSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testUndeploy() throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache cache = ignite.createCache(cacheConfiguration());

        for (int k = 0; k < 100; k++)
            cache.put(k, new TestType());

        GridUnsafeMemory mem = schemaMemory(ignite, cache.getName());

        assertTrue(mem.allocatedSize() > 0);

        ((IgniteKernal)ignite).context().query().onUndeploy("cache", U.detectClassLoader(TestType.class));

        assertEquals(0, mem.allocatedSize());
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return Memory.
     */
    private GridUnsafeMemory schemaMemory(Ignite ignite, String cacheName) {
        Map<String, Object> schemas = GridTestUtils.getFieldValue(((IgniteKernal)ignite).context().query(),
            "idx",
            "schemas");

        assertNotNull(schemas);

        Object schema = schemas.get("\"" + cacheName + "\"");

        assertNotNull(schema);

        GridUnsafeMemory mem = GridTestUtils.getFieldValue(schema, "offheap");

        assertNotNull(mem);

        return mem;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("cache");
        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setIndexedTypes(Integer.class, TestType.class);

        return ccfg;
    }

    /**
     *
     */
    static class TestType {
        /** */
        @QuerySqlField(index = true)
        private int v1;

        /** */
        @QuerySqlField(index = true)
        private String v2;

        /** */
        @QueryTextField
        private String v3;

        /**
         *
         */
        public TestType() {
            int v = ThreadLocalRandom.current().nextInt();

            v1 = v;
            v2 = String.valueOf(v);
            v3 = v2;
        }
    }
}
