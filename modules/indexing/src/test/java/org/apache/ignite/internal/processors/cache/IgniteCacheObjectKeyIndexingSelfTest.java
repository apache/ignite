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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test index behavior when key is of plain Object type per indexing settings.
 */
public class IgniteCacheObjectKeyIndexingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_SIZE = 20_000;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        Ignition.stopAll(true);

        cleanPersistenceDir();
    }

    /** */
    protected static CacheConfiguration<Object, TestObject> cacheCfg() {
        return new CacheConfiguration<Object, TestObject>(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setIndexedTypes(Object.class, TestObject.class);
    }

    /** */
    @Test
    public void testObjectKeyHandling() throws Exception {
        Ignite ignite = startGrid();

        IgniteCache<Object, TestObject> cache = ignite.getOrCreateCache(cacheCfg());

        UUID uid = UUID.randomUUID();

        cache.put(uid, new TestObject("A"));

        assertItemsNumber(1);

        cache.put(1, new TestObject("B"));

        assertItemsNumber(2);

        cache.put(uid, new TestObject("C"));

        // Key should have been replaced
        assertItemsNumber(2);

        List<List<?>> res = cache.query(new SqlFieldsQuery("select _key, name from TestObject order by name")).getAll();

        assertEquals(res,
            Arrays.asList(
                Arrays.asList(1, "B"),
                Arrays.asList(uid, "C")
            )
        );

        cache.remove(1);

        assertItemsNumber(1);

        res = cache.query(new SqlFieldsQuery("select _key, name from TestObject")).getAll();

        assertEquals(res,
            Collections.singletonList(
                Arrays.asList(uid, "C")
            )
        );

        cache.remove(uid);

        // Removal has worked for both keys although the table was the same and keys were of different type
        assertItemsNumber(0);
    }

    /** */
    @Test
    public void testObjectKeyHandlingDuringRebalance() throws Exception {
        startGrid(getTestIgniteInstanceName(0), createIgniteCfg(0));

        Ignite ign1 = startGrid(getTestIgniteInstanceName(1), createIgniteCfg(1));

        ign1.cluster().active(true);

        ign1.getOrCreateCache(cacheCfg());

        Map<TestObject, TestObject> before = fillCache(1, false, () -> false);

        stopGrid(1);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        cleanPersistenceDir("node_1");

        startGrid(getTestIgniteInstanceName(1), createIgniteCfg(1));

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        IgniteInternalFuture<?> fut = grid(1).cachex(DEFAULT_CACHE_NAME).rebalance();

        Map<TestObject, TestObject> after = fillCache(2, true, fut::isDone);

        // If this assertion fails, then we need to either correct the size of the rebalance batch,
        // or add rebalance throttling, or increase the size of the cache.
        Assert.assertFalse("Nothing was inserted during rebalance.", after.isEmpty());

        fut.get(getTestTimeout());

        IgniteCache<Object, TestObject> cache = grid(1).cache(DEFAULT_CACHE_NAME);

        for (Map.Entry<TestObject, TestObject> entry : before.entrySet()) {
            TestObject exp = after.getOrDefault(entry.getKey(), entry.getValue());

            List<List<?>> res = cache.query(new SqlFieldsQuery("select _val from TestObject where _key = ?")
                .setArgs(entry.getKey())).getAll();

            Assert.assertEquals(createSingleColumnResult(exp), res);
        }
    }

    /**
     * Creates result set with one row and one column with provided value.
     *
     * @param val Value.
     *
     * @return Created result set.
     */
    private static List<List<?>> createSingleColumnResult(TestObject val) {
        return Collections.singletonList(Collections.singletonList(val));
    }

    /** */
    private Map<TestObject, TestObject> fillCache(int mul, boolean rnd, Supplier<Boolean> stop) {
        log.info("Going to fill the cache");

        IgniteCache<Object, TestObject> cache = grid(1).getOrCreateCache(DEFAULT_CACHE_NAME);

        Map<TestObject, TestObject> res = new HashMap<>();

        int i = 0;
        for (; i < CACHE_SIZE; i++) {
            if (stop.get())
                break;

            int val = rnd ? ThreadLocalRandom.current().nextInt(CACHE_SIZE) : i;

            TestObject tstObj = new TestObject(String.valueOf(val * mul));

            cache.put(tstObj, tstObj);
            res.put(tstObj, tstObj);

            if ((i + 1) % 1000 == 0)
                log.info("\t-> " + (i + 1) + " entries of " + CACHE_SIZE + " has been inserted");
        }

        log.info("Cache is filled with " + i + " entries");

        return res;
    }

    /** */
    private IgniteConfiguration createIgniteCfg(int id) {
        return new IgniteConfiguration()
            .setGridLogger(log)
            .setConsistentId("node_" + id)
            .setRebalanceBatchSize(64)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            );
    }

    /** */
    @SuppressWarnings("ConstantConditions")
    private void assertItemsNumber(long num) {
        assertEquals(num, grid().cachex(DEFAULT_CACHE_NAME).size());

        assertEquals(num, grid().cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select count(*) from TestObject")).getAll()
            .get(0).get(0));
    }

    /** */
    private static class TestObject {
        /** */
        @QuerySqlField
        @GridToStringInclude
        public final String name;

        /** */
        private TestObject(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject obj = (TestObject)o;

            return Objects.equals(name, obj.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestObject.class, this);
        }
    }
}
