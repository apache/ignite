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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.AbstractContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/** Tests that exception in user code of ContinuousQuery doesn't affect transactions or node functionality. */
@RunWith(Parameterized.class)
public class ContinuousQueryUserCodeExceptionTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode cacheAtomicity;

    /** */
    @Parameterized.Parameter(2)
    public boolean locQry;

    /** */
    @Parameterized.Parameters(name = "cacheMode={0}, atomicityMode={1}, local={2}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            F.asList(REPLICATED, PARTITIONED),
            F.asList(ATOMIC, TRANSACTIONAL),
            F.asList(false, true)
        );
    }

    /** */
    private final AtomicBoolean fail = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(cacheMode)
            .setAtomicityMode(cacheAtomicity);

        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler((ignite, ctx) -> { fail.set(true); return true; })
            .setCacheConfiguration(cacheCfg);
    }

    /** */
    @Test
    public void testExceptionInLocalListener() throws Exception {
        checkCQ(new ContinuousQuery<>().setLocalListener(evts -> { throw new RuntimeException(); }));
    }

    /** */
    @Test
    public void testExceptionInRemoteFilter() throws Exception {
        checkCQ(new ContinuousQuery<>().setRemoteFilterFactory(() -> evts -> { throw new RuntimeException(); }));
    }

    /** */
    @Test
    public void testExceptionInTransformer() throws Exception {
        checkCQ(new ContinuousQueryWithTransformer<>().setLocalListener(evts -> {})
            .setRemoteTransformerFactory(() -> e -> { throw new RuntimeException(); }));
    }

    /** */
    private void checkCQ(AbstractContinuousQuery<?, ?> cq) throws Exception{
        fail.set(false);

        if (locQry)
            cq.setLocal(true);

        IgniteEx ignite = startGrids(2);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.query(cq);

        if (cacheAtomicity == TRANSACTIONAL) {
            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < 100; i++)
                    cache.put(i, 0);

                tx.commit();
            }
        }
        else {
            for (int i = 0; i < 100; i++)
                cache.put(i, 0);
        }

        assertFalse(fail.get());
    }
}
