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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsTableScan;

/**
 * Test query expired entries.
 */
public class ExpiredEntriesIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Override protected void beforeTest() throws Exception {
        CacheConfiguration<Integer, Developer> cacheCfg = new CacheConfiguration<Integer, Developer>()
            .setIndexedTypes(Integer.class, Developer.class)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 1)));

        client.createCache(new CacheConfiguration<>(cacheCfg)
            .setName("CACHE1")
            .setEagerTtl(false)
        );

        client.createCache(new CacheConfiguration<>(cacheCfg)
            .setName("CACHE2")
            .setEagerTtl(true)
        );

        awaitPartitionMapExchange();
    }

    /** */
    @Test
    public void testExpiration() throws Exception {
        IgniteCache<Integer, Developer> cache1 = client.cache("CACHE1");
        IgniteCache<Integer, Developer> cache2 = client.cache("CACHE2");

        for (int i = 0; i < 100; i++) {
            cache1.put(i, new Developer("dev" + i, i));
            cache2.put(i, new Developer("dev" + i, i));
        }

        ExpiryPolicy expPlc = new CreatedExpiryPolicy(new Duration(TimeUnit.DAYS, 1));

        for (int i = 50; i < 55; i++) {
            cache1.withExpiryPolicy(expPlc).put(i, new Developer("dev" + i, i));
            cache2.withExpiryPolicy(expPlc).put(i, new Developer("dev" + i, i));
        }

        GridTestUtils.waitForCondition(() -> cache2.size() == 5, 1_000);

        checkExpiration("CACHE1", false);
        checkExpiration("CACHE2", true);
    }

    /** Validate that check of expiry policy doesn't require additional page read. */
    @Test
    public void testIndexScanReadPageOnce() {
        for (String cacheName: F.asList("CACHE1", "CACHE2")) {
            IgniteCache<Integer, Developer> cache = client.cache(cacheName);

            ExpiryPolicy expPlc = new CreatedExpiryPolicy(new Duration(TimeUnit.DAYS, 1));

            int key = primaryKey(grid(0).cache(cacheName));

            cache.withExpiryPolicy(expPlc).put(key, new Developer("dev0", 0));

            IoStatisticsHolder statHld = grid(0).cachex(cacheName).context().group().statisticsHolderData();

            long before = statHld.logicalReads();

            assertQuery("SELECT /*+ FORCE_INDEX(DEVELOPER_DEPID_IDX) */ name, depId FROM " + cacheName + ".DEVELOPER where depId=0")
                .matches(QueryChecker.containsIndexScan(cacheName, "DEVELOPER", "DEVELOPER_DEPID_IDX"))
                .check();

            assertEquals(1, statHld.logicalReads() - before);
        }
    }

    /** */
    private void checkExpiration(String schema, boolean eagerTtl) {
        assertQuery("SELECT depId, name FROM " + schema + ".DEVELOPER WHERE name IS NOT NULL")
            .matches(containsTableScan(schema, "DEVELOPER"))
            .returns(50, "dev50").returns(51, "dev51").returns(52, "dev52").returns(53, "dev53").returns(54, "dev54")
            .check();

        assertQuery("SELECT depId, name FROM " + schema + ".DEVELOPER WHERE depId BETWEEN 30 and 70")
            .matches(containsIndexScan(schema, "DEVELOPER"))
            .returns(50, "dev50").returns(51, "dev51").returns(52, "dev52").returns(53, "dev53").returns(54, "dev54")
            .check();

        assertQuery("SELECT depId FROM " + schema + ".DEVELOPER WHERE depId BETWEEN 30 and 70")
            .matches(containsSubPlan("inlineScan=[" + eagerTtl + "]"))
            .returns(50).returns(51).returns(52).returns(53).returns(54)
            .check();

        assertQuery("SELECT min(depId) FROM " + schema + ".DEVELOPER")
            .matches(containsSubPlan("IgniteIndexBound"))
            .returns(50)
            .check();

        assertQuery("SELECT max(depId) FROM " + schema + ".DEVELOPER")
            .matches(containsSubPlan("IgniteIndexBound"))
            .returns(54)
            .check();

        assertQuery("SELECT count(depId) FROM " + schema + ".DEVELOPER")
            .matches(containsSubPlan("IgniteIndexCount"))
            .returns(5L)
            .check();

        assertQuery("SELECT count(*) FROM " + schema + ".DEVELOPER")
            .matches(containsSubPlan("IgniteIndexCount"))
            .returns(5L)
            .check();
    }

    /** */
    private static class Developer {
        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField(index = true)
        int depId;

        /** */
        public Developer(String name, int depId) {
            this.name = name;
            this.depId = depId;
        }
    }
}
