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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/** */
@RunWith(Parameterized.class)
public class CacheQueryFilterExpiredTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode cacheAtomicityMode;

    /** */
    @Parameterized.Parameter(2)
    public boolean eagerTtl;

    /** */
    @Parameterized.Parameters(name = "cacheMode={0}, atomicityMode={1}, eagerTtl={2}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        Stream.of(REPLICATED, PARTITIONED).forEach(cacheMode ->
            Stream.of(ATOMIC, TRANSACTIONAL).forEach(cacheAtomicityMode ->
                Stream.of(false, true).forEach(eagerTtl ->
                    params.add(new Object[] {cacheMode, cacheAtomicityMode, eagerTtl})
                )
            )
        );

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFilterExpired() throws Exception {
        try (Ignite ignite = startGrids(2)) {
            checkFilterExpired(ignite);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertExpired() throws Exception {
        try (Ignite ignite = startGrids(2)) {
            checkInsertExpired(ignite);
        }
    }

    /**
     * @param ignite Node.
     * @throws Exception If failed.
     */
    private void checkFilterExpired(Ignite ignite) throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(eagerTtl);
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        final IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        try {
            IgniteCache<Integer, Integer> expCache =
                cache.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(0, 2000)));

            for (int i = 0; i < 10; i++) {
                IgniteCache<Integer, Integer> cache0 = i % 2 == 0 ? cache : expCache;

                cache0.put(i, i);
            }

            assertEquals(10, cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll().size());
            assertEquals(10, cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll().size());

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll().size() == 5 &&
                        cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll().size() == 5;
                }
            }, 5000);

            assertEquals(5, cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll().size());
            assertEquals(5, cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll().size());
        }
        finally {
            ignite.destroyCache(ccfg.getName());
        }
    }

    /** */
    private void checkInsertExpired(Ignite ignite) throws Exception {
        CacheConfiguration<Integer, Integer> ccfgFrom = new CacheConfiguration<>("CACHE1");
        ccfgFrom.setAtomicityMode(cacheAtomicityMode);
        ccfgFrom.setCacheMode(cacheMode);
        ccfgFrom.setEagerTtl(eagerTtl);
        ccfgFrom.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY));
        ccfgFrom.setIndexedTypes(Integer.class, Integer.class);

        CacheConfiguration<Integer, Integer> ccfgTo = new CacheConfiguration<>("CACHE2");
        ccfgTo.setAtomicityMode(cacheAtomicityMode);
        ccfgTo.setCacheMode(cacheMode);
        ccfgTo.setEagerTtl(eagerTtl);
        ccfgTo.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 2_000)));
        ccfgTo.setIndexedTypes(Integer.class, Integer.class);

        final IgniteCache<Integer, Integer> cacheFrom = ignite.createCache(ccfgFrom);
        final IgniteCache<Integer, Integer> cacheTo = ignite.createCache(ccfgTo);

        for (int i = 0; i < 10; i++)
            cacheFrom.put(i, i);

        cacheFrom.query(new SqlFieldsQuery("INSERT INTO cache2.INTEGER(_key, _val) SELECT _key, _val FROM cache1.INTEGER;")).getAll();

        assertEquals(10, cacheTo.query(new SqlFieldsQuery("select _key, _val from cache2.INTEGER")).getAll().size());

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cacheTo.query(new SqlFieldsQuery("select _key, _val from cache2.INTEGER")).getAll().isEmpty();
            }
        }, 5_000, 1_000));
    }
}
